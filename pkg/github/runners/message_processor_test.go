package runners

import (
	"context"
	"encoding/json"
	"sync/atomic"

	"github.com/macstadium/orka-github-actions-integration/pkg/github/types"
	"github.com/macstadium/orka-github-actions-integration/pkg/logging"
	"github.com/macstadium/orka-github-actions-integration/pkg/orka"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
)

type MockRunnerManager struct {
	AcquireJobsFunc      func(ctx context.Context, requestIds []int64) error
	GetAcquirableJobFunc func(ctx context.Context) (*types.AcquirableJobList, error)
	AcquiredJobIds       []int64
}

func (m *MockRunnerManager) ProcessMessages(ctx context.Context, handler func(msg *types.RunnerScaleSetMessage) error) error {
	return nil
}

func (m *MockRunnerManager) AcquireJobs(ctx context.Context, requestIds []int64) error {
	m.AcquiredJobIds = append(m.AcquiredJobIds, requestIds...)
	if m.AcquireJobsFunc != nil {
		return m.AcquireJobsFunc(ctx, requestIds)
	}
	return nil
}

func (m *MockRunnerManager) GetAcquirableJobs(ctx context.Context) (*types.AcquirableJobList, error) {
	if m.GetAcquirableJobFunc != nil {
		return m.GetAcquirableJobFunc(ctx)
	}
	return &types.AcquirableJobList{Count: 0, Jobs: []types.AcquirableJob{}}, nil
}

type MockRunnerProvisioner struct {
	ProvisionCount atomic.Int32
	ProvisionFunc  func(ctx context.Context) (*orka.VMCommandExecutor, []string, error)
}

func (m *MockRunnerProvisioner) ProvisionRunner(ctx context.Context) (*orka.VMCommandExecutor, []string, error) {
	m.ProvisionCount.Add(1)
	if m.ProvisionFunc != nil {
		return m.ProvisionFunc(ctx)
	}
	return &orka.VMCommandExecutor{
		VMName: "test-vm",
		Logger: zap.NewNop().Sugar(),
	}, []string{"echo ok"}, nil
}

func (m *MockRunnerProvisioner) CleanupResources(ctx context.Context, runnerName string) {}

func buildMessage(messageId int64, stats *types.RunnerScaleSetStatistic, jobMessages ...any) *types.RunnerScaleSetMessage {
	body, _ := json.Marshal(jobMessages)
	return &types.RunnerScaleSetMessage{
		MessageId:   messageId,
		MessageType: "RunnerScaleSetJobMessages",
		Statistics:  stats,
		Body:        string(body),
	}
}

var _ = Describe("RunnerMessageProcessor", func() {
	var (
		processor   *RunnerMessageProcessor
		mockManager *MockRunnerManager
		mockProv    *MockRunnerProvisioner
		tracker     *VMTracker
		ctx         context.Context
		mockOrka    *MockOrkaClient
		mockActions *MockActionsClient
	)

	BeforeEach(func() {
		logging.SetupLogger("info")
		ctx = context.Background()
		mockManager = &MockRunnerManager{}
		mockProv = &MockRunnerProvisioner{}
		mockOrka = &MockOrkaClient{}
		mockActions = &MockActionsClient{}
		logger := zap.NewNop().Sugar()
		tracker = NewVMTracker(mockOrka, mockActions, logger)

		processor = NewRunnerMessageProcessor(
			ctx,
			mockManager,
			mockProv,
			tracker,
			&types.RunnerScaleSet{Id: 1, Name: "test-runner"},
		)
	})

	Describe("processRunnerMessage", func() {
		Context("deficit recovery for stranded assigned jobs", func() {
			It("should provision runners for assigned jobs found via GetAcquirableJobs when batch has no JobAssigned", func() {
				provisionCh := make(chan struct{})
				mockProv.ProvisionFunc = func(ctx context.Context) (*orka.VMCommandExecutor, []string, error) {
					<-provisionCh
					return nil, nil, context.Canceled
				}

				mockManager.GetAcquirableJobFunc = func(ctx context.Context) (*types.AcquirableJobList, error) {
					return &types.AcquirableJobList{
						Count: 3,
						Jobs: []types.AcquirableJob{
							{RunnerRequestId: 10, MessageType: "JobAssigned"},
							{RunnerRequestId: 11, MessageType: "JobAssigned"},
							{RunnerRequestId: 12, MessageType: "JobAssigned"},
						},
					}, nil
				}

				stats := &types.RunnerScaleSetStatistic{
					TotalAssignedJobs:      5,
					TotalRegisteredRunners: 2,
				}

				msg := buildMessage(100, stats, types.JobCompleted{
					JobMessageBase: types.JobMessageBase{
						JobMessageType:  types.JobMessageType{MessageType: "JobCompleted"},
						RunnerRequestId: 999,
					},
					Result:     "succeeded",
					RunnerName: "test-runner-abc",
				})

				err := processor.processRunnerMessage(msg)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() int32 {
					return mockProv.ProvisionCount.Load()
				}).Should(Equal(int32(3)), "Should provision 3 runners for the deficit (5 assigned - 2 registered = 3)")

				close(provisionCh)
			})

			It("should not over-provision when goroutines are already in-flight", func() {
				provisionCh := make(chan struct{})
				mockProv.ProvisionFunc = func(ctx context.Context) (*orka.VMCommandExecutor, []string, error) {
					<-provisionCh
					return nil, nil, context.Canceled
				}

				mockManager.GetAcquirableJobFunc = func(ctx context.Context) (*types.AcquirableJobList, error) {
					Fail("GetAcquirableJobs should not be called when in-flight covers the deficit")
					return nil, nil
				}

				// Simulate 8 goroutines already in-flight from previous batches
				processor.inFlightProvisioning.Store(8)

				stats := &types.RunnerScaleSetStatistic{
					TotalAssignedJobs:      10,
					TotalRegisteredRunners: 0,
				}

				msg := buildMessage(200, stats,
					types.JobAssigned{
						JobMessageBase: types.JobMessageBase{
							JobMessageType:  types.JobMessageType{MessageType: "JobAssigned"},
							JobId:           "job-1",
							RunnerRequestId: 1,
						},
					},
					types.JobAssigned{
						JobMessageBase: types.JobMessageBase{
							JobMessageType:  types.JobMessageType{MessageType: "JobAssigned"},
							JobId:           "job-2",
							RunnerRequestId: 2,
						},
					},
				)

				err := processor.processRunnerMessage(msg)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() int32 {
					return mockProv.ProvisionCount.Load()
				}).Should(Equal(int32(2)), "Should only provision 2 from the batch (10 - 0 - 8 = 2 required)")

				close(provisionCh)
			})

			It("should skip deficit recovery when in-flight goroutines already cover all assigned jobs", func() {
				mockManager.GetAcquirableJobFunc = func(ctx context.Context) (*types.AcquirableJobList, error) {
					Fail("GetAcquirableJobs should not be called when there is no deficit")
					return nil, nil
				}

				// 10 goroutines already in-flight
				processor.inFlightProvisioning.Store(10)

				stats := &types.RunnerScaleSetStatistic{
					TotalAssignedJobs:      12,
					TotalRegisteredRunners: 2,
				}

				msg := buildMessage(300, stats, types.JobCompleted{
					JobMessageBase: types.JobMessageBase{
						JobMessageType:  types.JobMessageType{MessageType: "JobCompleted"},
						RunnerRequestId: 888,
					},
					Result:     "succeeded",
					RunnerName: "test-runner-xyz",
				})

				err := processor.processRunnerMessage(msg)
				Expect(err).NotTo(HaveOccurred())

				Expect(mockProv.ProvisionCount.Load()).To(Equal(int32(0)),
					"No new provisioning should happen when in-flight (10) + registered (2) >= assigned (12)")
			})

			It("should only provision for JobAssigned acquirable jobs, not JobAvailable", func() {
				provisionCh := make(chan struct{})
				mockProv.ProvisionFunc = func(ctx context.Context) (*orka.VMCommandExecutor, []string, error) {
					<-provisionCh
					return nil, nil, context.Canceled
				}

				mockManager.GetAcquirableJobFunc = func(ctx context.Context) (*types.AcquirableJobList, error) {
					return &types.AcquirableJobList{
						Count: 3,
						Jobs: []types.AcquirableJob{
							{RunnerRequestId: 10, MessageType: "JobAvailable"},
							{RunnerRequestId: 11, MessageType: "JobAssigned"},
							{RunnerRequestId: 12, MessageType: "JobAvailable"},
						},
					}, nil
				}

				stats := &types.RunnerScaleSetStatistic{
					TotalAssignedJobs:      5,
					TotalRegisteredRunners: 0,
				}

				msg := buildMessage(400, stats, types.JobCompleted{
					JobMessageBase: types.JobMessageBase{
						JobMessageType:  types.JobMessageType{MessageType: "JobCompleted"},
						RunnerRequestId: 777,
					},
					Result:     "succeeded",
					RunnerName: "test-runner-zzz",
				})

				err := processor.processRunnerMessage(msg)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() int32 {
					return mockProv.ProvisionCount.Load()
				}).Should(Equal(int32(1)), "Should only provision for the 1 JobAssigned, not the 2 JobAvailable")

				close(provisionCh)
			})
		})
	})
})
