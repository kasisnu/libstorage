package objectivefs

import (
	"github.com/akutz/gofig"
)

const (
	// Name is the provider's name.
	Name = "objectivefs"
)

func init() {
	registerConfig()
}

func registerConfig() {
	r := gofig.NewRegistration("OBJECTIVEFS")
	r.Key(gofig.String, "", "", "", "objectivefs.accessKey")
	r.Key(gofig.String, "", "", "", "objectivefs.secretKey")
	r.Key(gofig.String, "", "", "", "objectivefs.metadataHost")
	r.Key(gofig.String, "", "", "", "objectivefs.license")
	r.Key(gofig.String, "", "", "", "objectivefs.adminLicense")
	r.Key(gofig.String, "", "", "", "objectivefs.passphrase")
	r.Key(gofig.String, "", "us-east-1", "AWS region", "objectivefs.region")
	gofig.Register(r)
}
