package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"

	pb "file_transfer/proto/idl/file_transfer" // 修改包路径以适应您的生成路径
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// server 是实现了 FileTransferServer 接口的服务器
type server struct {
	pb.UnimplementedFileTransferServer
}

// UploadFile 实现了文件上传的流式 RPC 方法
func (s *server) UploadFile(stream pb.FileTransfer_UploadFileServer) error {
	var totalSize int64
	var lastOffset int64
	tempDir := "/Users/hanzhongqing/Workspace/file_transfer/data/temp_chunks"              // 临时目录，用于存储每个切片
	mergedFileName := "/Users/hanzhongqing/Workspace/file_transfer/data/uploaded_file.mkv" // 合并后的文件名

	// 创建临时目录
	if err := os.MkdirAll(tempDir, os.ModePerm); err != nil {
		return status.Errorf(codes.Internal, "Failed to create temp directory: %v", err)
	}
	// defer os.RemoveAll(tempDir) // 在函数返回时清理临时目录

	chunkFiles := []string{}

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			// 所有块已接收，检查并合并所有块
			if err := mergeChunks(chunkFiles, mergedFileName); err != nil {
				return status.Errorf(codes.Internal, "Failed to merge chunks: %v", err)
			}
			log.Printf("File received, total size: %d bytes", totalSize)
			return stream.SendAndClose(&pb.UploadResponse{Success: true, Message: "File uploaded successfully"})
		}
		if err != nil {
			return err
		}

		// 检查块的连续性
		if chunk.Offset != lastOffset {
			return status.Errorf(codes.InvalidArgument, "Invalid chunk received, expected offset %d but got %d", lastOffset, chunk.Offset)
		}
		lastOffset = chunk.Offset + int64(chunk.ChunkSize)

		// 将块内容写入临时文件
		chunkFileName := filepath.Join(tempDir, fmt.Sprintf("chunk_%d", chunk.Offset))
		if err := os.WriteFile(chunkFileName, chunk.Data, os.ModePerm); err != nil {
			return status.Errorf(codes.Internal, "Failed to write chunk to file: %v", err)
		}
		chunkFiles = append(chunkFiles, chunkFileName)
		totalSize += int64(chunk.ChunkSize)
	}
}

// mergeChunks 合并所有切片文件成一个完整文件
func mergeChunks(chunkFiles []string, mergedFileName string) error {
	// 按文件名排序，以确保按顺序合并
	sort.Strings(chunkFiles)

	mergedFile, err := os.Create(mergedFileName)
	if err != nil {
		return fmt.Errorf("failed to create merged file: %v", err)
	}
	defer mergedFile.Close()

	for _, chunkFile := range chunkFiles {
		data, err := os.ReadFile(chunkFile)
		if err != nil {
			return fmt.Errorf("failed to read chunk file: %v", err)
		}
		if _, err := mergedFile.Write(data); err != nil {
			return fmt.Errorf("failed to write to merged file: %v", err)
		}
	}

	return nil
}

func main() {
	// 启动 gRPC 服务器
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterFileTransferServer(s, &server{})

	fmt.Println("Server is running on port :50051...")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
