module github.com/go-anyway/framework-configcenter

go 1.25.4

require (
	github.com/go-anyway/framework-config v1.0.0
	github.com/go-anyway/framework-hotreload v1.0.0
	github.com/go-anyway/framework-log v1.0.0
	github.com/go-anyway/framework-trace v1.0.0
	github.com/apolloconfig/agollo/v4 v4.4.0
	github.com/nacos-group/nacos-sdk-go/v2 v2.3.5
	github.com/hashicorp/consul/api v1.33.0
)

replace (
	github.com/go-anyway/framework-config => ../core/config
	github.com/go-anyway/framework-hotreload => ../hotreload
	github.com/go-anyway/framework-log => ../core/log
	github.com/go-anyway/framework-trace => ../trace
)
