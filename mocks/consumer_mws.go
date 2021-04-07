// Code generated by MockGen. DO NOT EDIT.
// Source: consumer_mws.go

// Package mocks is a generated GoMock package.
package mocks

import (
	reflect "reflect"
	time "time"

	gomock "github.com/golang/mock/gomock"
	mws "go.bukalapak.io/mws-api-go-client"
)

// MockMws is a mock of Mws interface.
type MockMws struct {
	ctrl     *gomock.Controller
	recorder *MockMwsMockRecorder
}

// MockMwsMockRecorder is the mock recorder for MockMws.
type MockMwsMockRecorder struct {
	mock *MockMws
}

// NewMockMws creates a new mock instance.
func NewMockMws(ctrl *gomock.Controller) *MockMws {
	mock := &MockMws{ctrl: ctrl}
	mock.recorder = &MockMwsMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockMws) EXPECT() *MockMwsMockRecorder {
	return m.recorder
}

// PutJobWithID mocks base method.
func (m *MockMws) PutJobWithID(jobName, jobID, routingKey, priority string, payload mws.Payload, delay int64, timeout time.Duration) (*mws.APIResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PutJobWithID", jobName, jobID, routingKey, priority, payload, delay, timeout)
	ret0, _ := ret[0].(*mws.APIResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PutJobWithID indicates an expected call of PutJobWithID.
func (mr *MockMwsMockRecorder) PutJobWithID(jobName, jobID, routingKey, priority, payload, delay, timeout interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PutJobWithID", reflect.TypeOf((*MockMws)(nil).PutJobWithID), jobName, jobID, routingKey, priority, payload, delay, timeout)
}