package s3fs

import (
	"github.com/akutz/gofig"
)

const (
	// Name is the provider's name.
	Name = "s3fs"
)

func init() {
	registerConfig()
}

func registerConfig() {
	r := gofig.NewRegistration("S3FS")
	r.Key(gofig.String, "", "", "", "s3fs.accessKey")
	r.Key(gofig.String, "", "", "", "s3fs.secretKey")
	//r.Key(gofig.String, "", "", "Comma separated security group ids", "s3fs.securityGroups")
	r.Key(gofig.String, "", "us-east-1", "AWS region", "s3fs.region")
	r.Key(gofig.String, "", "", "Tag prefix for S3FS naming", "s3fs.tag")
	r.Key(gofig.String, "", "s3fs", "Objectivefs/s3fs", "s3fs.type")
	gofig.Register(r)
}
