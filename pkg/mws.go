package pkg

//go:generate mockgen -source=mws.go -destination=mocks/mws.go -package=mocks

import (
	"time"

	mws "go.bukalapak.io/mws-api-go-client"
)

type Mws interface {
	PutJobWithID(jobName, jobID, routingKey, priority string, payload mws.Payload, delay int64, timeout time.Duration) (resp *mws.APIResponse, err error)
}
