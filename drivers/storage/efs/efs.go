package efs

import (
	"github.com/akutz/gofig"
)

const (
	// Name is the provider's name.
	Name = "efs"
)

func init() {
	registerConfig()
}

func registerConfig() {
	r := gofig.NewRegistration("EFS")
	r.Key(gofig.String, "", "", "", "efs.accessKey")
	r.Key(gofig.String, "", "", "", "efs.secretKey")
	r.Key(gofig.String, "", "", "", "efs.securityGroups")
	r.Key(gofig.String, "", "", "", "efs.region")
	r.Key(gofig.String, "", "", "", "efs.tag")
	gofig.Register(r)
}
