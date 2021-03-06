package services

import (
	gofig "github.com/akutz/gofig/types"
	"github.com/akutz/goof"

	"github.com/codedellemc/libstorage/api/context"
	"github.com/codedellemc/libstorage/api/registry"
	"github.com/codedellemc/libstorage/api/types"
)

type storageService struct {
	name          string
	driver        types.StorageDriver
	config        gofig.Config
	taskExecQueue chan *task
}

func (s *storageService) Init(ctx types.Context, config gofig.Config) error {
	s.config = config

	if err := s.initStorageDriver(ctx); err != nil {
		return err
	}

	s.taskExecQueue = make(chan *task)
	go func() {
		for t := range s.taskExecQueue {
			execTask(t)
		}
	}()
	return nil
}

func (s *storageService) initStorageDriver(ctx types.Context) error {
	driverName := s.config.GetString("driver")
	if driverName == "" {
		driverName = s.config.GetString("libstorage.driver")
		if driverName == "" {
			driverName = s.config.GetString("libstorage.storage.driver")
			if driverName == "" {
				return goof.WithField(
					"service", s.name, "error getting driver name")
			}
		}
	}

	ctx.WithField("driverName", driverName).Debug("got driver name")
	driver, err := registry.NewStorageDriver(driverName)
	if err != nil {
		return err
	}

	ctx = ctx.WithValue(context.DriverKey, driver)

	if err := driver.Init(ctx, s.config); err != nil {
		return err
	}

	s.driver = driver
	return nil
}

func (s *storageService) Config() gofig.Config {
	return s.config
}

func (s *storageService) Driver() types.StorageDriver {
	return s.driver
}

func (s *storageService) TaskExecute(
	ctx types.Context,
	run types.StorageTaskRunFunc,
	schema []byte) *types.Task {

	t := newStorageServiceTask(ctx, run, s, schema)
	go func() { s.taskExecQueue <- t }()
	return &t.Task
}

func (s *storageService) Name() string {
	return s.name
}
