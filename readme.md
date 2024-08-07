## 大文件通过 grpc, 分片 传输demo


### 编译 proto

protoc --go_out=./proto --go-grpc_out=./proto proto/file.proto


### 启动服务端

go run server/main.go


### 启动客户端

go run client/main.go



### todo

错误重传