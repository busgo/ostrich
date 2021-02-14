# ostrich
基于grcp微服务框架

1. proto文件生成

```shell script
  protoc -I ./proto   --go_out=plugins=grpc:./proto/pb  ./proto/*.proto
```