/**
 * 进行本地文件上传bce-bos服务,后期可优化为直接进行bos存储
 */
package bosstore

 import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"strconv"
	"crypto/sha256"
	"math/rand"
	"time"

	"github.com/tus/tusd"
	"github.com/tus/tusd/uid"

	"github.com/baidubce/bce-sdk-go/services/bos"
	"github.com/baidubce/bce-sdk-go/bce"
	"github.com/baidubce/bce-sdk-go/services/bos/api"

	"github.com/logger"
 )
// // This regular expression matches every character which is not defined in the
// // ASCII tables which range from 00 to 7F, inclusive.
// var nonASCIIRegexp = regexp.MustCompile(`([^\x00-\x7F])`)

 type BosStore struct {
	//用户的Access Key 和 Secret Access Key
	AccessKeyID string
	SecretAccessKey string
	//用户指定的Endpoint
	Endpoint    string
	Client      *bos.Client
	//bucket 地址
	BucketNames  map[string]string
	// ObjectPrefix is prepended to the name of each S3 object that is created.
	// It can be used to create a pseudo-directory structure in the bucket,
	// e.g. "path/to/my/uploads".
	ObjectPrefix string
	// MaxPartSize specifies the maximum size of a single part uploaded to S3
	// in bytes. This value must be bigger than MinPartSize! In order to
	// choose the correct number, two things have to be kept in mind:
	//
	// If this value is too big and uploading the part to S3 is interrupted
	// expectedly, the entire part is discarded and the end user is required
	// to resume the upload and re-upload the entire big part. In addition, the
	// entire part must be written to disk before submitting to S3.
	//
	// If this value is too low, a lot of requests to S3 may be made, depending
	// on how fast data is coming in. This may result in an eventual overhead.
	MaxPartSize int64
	// MinPartSize specifies the minimum size of a single part uploaded to S3
	// in bytes. This number needs to match with the underlying S3 backend or else
	// uploaded parts will be reject. AWS S3, for example, uses 5MB for this value.
	MinPartSize int64
	// MaxMultipartParts is the maximum number of parts an S3 multipart upload is
	// allowed to have according to AWS S3 API specifications.
	// See: http://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
	MaxMultipartParts int64
	// MaxObjectSize is the maximum size an S3 Object can have according to S3
	// API specifications. See link above.
	MaxObjectSize int64
 }

var (
		defaultFilePerm = os.FileMode(0664)
		TEMPPREFIX = "tempdata/tusd-bos-tmp-"
	)

type BosUrlInfo struct {
	Url     string
}


// New constructs a new storage using the supplied bucket and service object.
func New(accessKeyID, secretAccessKey, endpoint string, bucketNames map[string]string) BosStore {
 return BosStore{
	 AccessKeyID:        accessKeyID,
	 SecretAccessKey:    secretAccessKey,
	 Endpoint:           endpoint,
	 BucketNames:         bucketNames,
	 MaxPartSize:        5 * 1024 * 1024 * 1024,
	 MinPartSize:        5 * 1024 * 1024,
	 MaxMultipartParts:  10000,
	 MaxObjectSize:      5 * 1024 * 1024 * 1024 * 1024,
 }
}

// UseIn sets this store as the core data store in the passed composer and adds
// all possible extension to it.
func (store BosStore) UseIn(composer *tusd.StoreComposer) {
 composer.UseCore(store)
 composer.UseTerminater(store)
 composer.UseFinisher(store)
 composer.UseGetReader(store)
 composer.UseConcater(store)
 composer.UseLengthDeferrer(store)
}

func (store BosStore) NewUpload(info tusd.FileInfo) (id string, err error) {
 // an upload larger than MaxObjectSize must throw an error
 if info.Size > store.MaxObjectSize {
	 return "", fmt.Errorf("bosStore: upload size of %v bytes exceeds MaxObjectSize of %v bytes", info.Size, store.MaxObjectSize)
 }

 var uploadId string
 if info.ID == "" {
	/*生成唯一ID*/
	prefix := ""
	filetype := ""
	if _, ok := info.MetaData["filetype"]; ok {
		filetype = info.MetaData["filetype"]
	}
	 uploadId = prefix + uid.Uid() + "." + filetype
 } else {
	 // certain tests set info.ID in advance
	 uploadId = info.ID
 }

	 // Create the actual multipart upload
	 //获取pid
	 if _, ok := info.MetaData["pid"]; !ok {
	 	logger.Warn("pid not found")
	 	logger.Info(logger.TAIHETagMethodOut, "NewUpload")
	 	return "", errors.New("pid empty")
	 }

	 pid := info.MetaData["pid"]
	 res, err := store.CreateMultipartUpload(uploadId, pid)
	 if err != nil {
		 return "", fmt.Errorf("bosstore: unable to create multipart upload:\n%s", err)
	 }

	id = uploadId + "+" + res.UploadId
	info.ID = id

	infoJson, err := json.Marshal(info)
	if err != nil {
		return "", err
	}

	// Create object on bos containing information about the file
	objectKey := uploadId + ".info"
	_, err = store.PutObject(objectKey, pid,  infoJson)
	if err != nil {
		return "", fmt.Errorf("bosstore: unable to create info file:\n%s", err)
	}

	// Create .bin file with no content
	// fileName := TEMPPREFIX + id
	// fileName := id
	// file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, defaultFilePerm)
	// if err != nil {
	//  if os.IsNotExist(err) {
	//      err = fmt.Errorf("upload directory does not exist: %s", fileName)
	//  }
	//  return "", err
	// }
	// defer file.Close()

	// writeInfo creates the file by itself if necessary
	// err = store.writeInfoLocal(fileName, info)


	return id, err
}
/*创建分片上传*/
func (store BosStore) CreateMultipartUpload (id, pid string) (*api.InitiateMultipartUploadResult, error) {
	logger.Info(logger.TAIHETagMethodIn, "CreateMultipartUpload")
	// bucketName := store.BucketName
	bucketName, err := store.getBucketName(pid)
	if err != nil {
		logger.Warn("getBucketName failed:", err)
		logger.Info(logger.TAIHETagMethodOut, "CreateMultipartUpload")
	}

	bosClient, err := store.getBosClient()
	if err != nil {
		logger.Info(logger.TAIHETagMethodOut, "CreateMultipartUpload")
		return nil, err
	}

	objectKey := id
	contentType := "application/octet-stream"
	args := new(api.InitiateMultipartUploadArgs)
	args.StorageClass = api.STORAGE_CLASS_COLD
	res, err := bosClient.InitiateMultipartUpload(bucketName, objectKey, contentType, args)
	// fmt.Printf("CreateMultipartUpload res:%+v\n", res)
	logger.Info(logger.TAIHETagCreateMultipartUpload, "CreateMultipartUpload res:", res)
	logger.Info(logger.TAIHETagMethodOut, "CreateMultipartUpload")
	return res, err
}
/*put简单的文件*/
func (store BosStore) PutObject (objectKey, pid string, objectValue []byte) (string, error) {
	logger.Info(logger.TAIHETagMethodIn, "PutObject")
	bucketName, err := store.getBucketName(pid)
	if err != nil {
		logger.Warn("getBucketName failed:", err)
		logger.Info(logger.TAIHETagMethodOut, "PutObject")
	}

	bosClient, err := store.getBosClient()
	if err != nil {
		logger.Info(logger.TAIHETagMethodOut, "PutObject")
		return "", err
	}

	// 从字节数组上传
	// bucketName := store.BucketName
	objectName := objectKey
	byteArr := objectValue
	etag, err := bosClient.PutObjectFromBytes(bucketName, objectName, byteArr, nil)
	return etag, err
}

// func (store BosStore) WriteChunk(id string, offset int64, src io.Reader) (int64, error) {
//  file, err := os.OpenFile(TEMPPREFIX + id , os.O_CREATE|os.O_WRONLY|os.O_APPEND, defaultFilePerm)
//  if err != nil {
//      return 0, err
//  }
//  defer file.Close()

//  n, err := io.Copy(file, src)

//  info, errInfo := store.GetInfo(id)
//  if errInfo != nil {
//      fmt.Printf("errInfo:%s\n", errInfo)
//      return n, errInfo
//  }


//  fileInfo, _ := file.Stat()
//  fileSize := fileInfo.Size()
//  if fileSize == info.Size {
//      defer os.Remove(file.Name())
//      defer os.Remove(file.Name() + ".info")
//      err = store.WriteChunkToBosV2(id, file.Name())
//  }

//  return n, err
// }

// /*先让用户将该分片上传到本地，然后再上传到BOS*/
// func (store BosStore) WriteChunkToBosV2 (id string, fileName string) (error) {
//  bosClient, err := store.getBosClient()
//  if err != nil {
//      return err
//  }

//  uploadId, _ := splitIds(id)
//  // 从本地文件上传
//  bucketName := store.BucketName
//  objectName := uploadId
//  bodyStream, err := bce.NewBodyFromFile(fileName)
//  _, err = bosClient.PutObject(bucketName, objectName, bodyStream, nil)
//  return err
// }


func (store BosStore) WriteChunk (id, pid string, offset int64, src io.Reader) (bytesUploaded int64, err error) {
	logger.Info(logger.TAIHETagMethodIn, "WriteChunk")
	uploadId, multipartId := splitIds(id)

	//Get the total size of the current upload
	info, errInfo := store.GetInfo(id, pid)
	if errInfo != nil {
		err = errInfo
		logger.Info(logger.TAIHETagMethodOut, "WriteChunk")
		return
	}
	// fmt.Printf("%+v\n", info)
	logger.Info("info information: info.ID:", info.ID, "\tinfo.Size:", info.Size, "\tinfo.Offset:", info.Offset, "\tinfo.IsPartial:", info.IsPartial,
		"\tinfo.IsFinal:", info.IsFinal, "\tPartialUploads len:", len(info.PartialUploads))
	size := info.Size
	bytesUploaded = int64(0)
	optimalPartSize, errPartSize := store.calcOptimalPartSize(size)
	if errPartSize != nil {
		err = errPartSize
		logger.Info(logger.TAIHETagMethodOut, "WriteChunk")
		return
	}

	// Get number of parts to generate next number
	parts, errParts := store.listAllParts(id, pid)
	if errParts != nil {
		// fmt.Printf("WriteChunk err:%+v\n", errParts)
		logger.Warn("WriteChunk err:", errParts)
		logger.Info(logger.TAIHETagMethodOut, "WriteChunk")
		err = errParts
		return
	}

	numParts := len(parts)
	nextPartNum := int64(numParts + 1)
	bosClient, err := store.getBosClient()
	if err != nil {
		logger.Warn("getBosClient err:", err)
		logger.Info(logger.TAIHETagMethodOut, "WriteChunk")
		return
	}

	for {
		//Create a temporary file to store the part in it
		file, errFile := ioutil.TempFile("", id)
		if errFile != nil {
			err = errFile
			logger.Warn("create tmep file failed err:", err)
			logger.Info(logger.TAIHETagMethodOut, "WriteChunk")
			return
		}// if errFile != nil
		defer os.Remove(file.Name())
		defer file.Close()

		limitedReader := io.LimitReader(src, optimalPartSize)
		n, errN := io.Copy(file, limitedReader)
		fileInfo, _ := file.Stat()
		fileSize := fileInfo.Size()
		//io.Copy does not return io.EOF, so we not have to handle it differently.
		if errN != nil {
			if errN == io.EOF {
				logger.Info("input stream empty ignore it")
			} else {
				err = errN
				logger.Warn("copy file failed err:", err)
			}

			logger.Info(logger.TAIHETagMethodOut, "WriteChunk")
			return
		}
		//If io.Copy is finished reading, it will always return (0, nil).
		if n == 0 {
			logger.Info(logger.TAIHETagMethodOut, "WriteChunk")
			return
		}

		// fmt.Printf("size:%+v\toffset:%+v\toptimalPartSize:%+v\tn:%+v\tfileSize:%+v\n", size, offset, optimalPartSize, n, fileSize)
		logger.Info("info size:", size, "\toffset:", offset, "\toptimalPartSize:", optimalPartSize, "\tn:", n, "\tfileSize:", fileSize)
		if !info.SizeIsDeferred {
			if (size - offset) <= optimalPartSize {
				if (size - offset) != n {
					logger.Warn("size - offset != n")
					logger.Info(logger.TAIHETagMethodOut, "WriteChunk")
					return
				}
			} else if n < optimalPartSize {
				logger.Warn("n < optimalPartSize")
				logger.Info(logger.TAIHETagMethodOut, "WriteChunk")
				return
			}//if (size - offset) <= optimalPartSize
		}//if ! info.SizeIsDeferred

		// Seek to the beginning of the file
		file.Seek(0, 0)
		objectKey := uploadId
		uploadIdArgs := multipartId
		for i := 0; i < 3; i++{
			err = store.UploadPart(file, objectKey, pid, uploadIdArgs, nextPartNum, bosClient)
			if err != nil {
				logger.Warn("UploadPart failed err:", err)
				continue
			}
			break
		}

		if err != nil {
			logger.Warn("UploadPart try executed 3 times but failed:current bytesUploaded:",
				bytesUploaded, "\toffset:", offset, "\tcurrentPartNum:", nextPartNum)
			logger.Info(logger.TAIHETagMethodOut, "WriteChunk")
			return
		}

		logger.Info("UploadPart done current bytesUploaded:", bytesUploaded, "\toffset:", offset, 
			"\tcurrentPartNum:", nextPartNum)
		offset += n
		bytesUploaded += n
		nextPartNum += 1
	}//for

	logger.Info(logger.TAIHETagMethodOut, "WriteChunk")
	return
}

func (store BosStore) GetInfo(id, pid string) (info tusd.FileInfo, err error) {
	// fmt.Printf("GetInfo\n")
	logger.Info(logger.TAIHETagMethodIn, "GetInfo")
	// //为了考虑效率,先将该分片放在本地，上传该分片完成后再上传BOS
	// return store.getInfoLocal(TEMPPREFIX + id)

	uploadId, _ := splitIds(id)

	//Get file info stored in separate object
	objectKey := uploadId + ".info"
	res, err := store.GetObject(objectKey, pid)
	if err != nil {
		logger.Warn("GetObject failed err:", err)
		logger.Info(logger.TAIHETagMethodOut, "GetInfo")
		return
	}

	if err := json.NewDecoder(res.Body).Decode(&info); err != nil {
		logger.Warn("decode info failed err:", err)
		logger.Info(logger.TAIHETagMethodOut, "GetInfo")
		return info, err
	}

	// Get uploaded parts and their offset
	parts, errPa := store.listAllParts(id, pid)
	if errPa != nil {
		// fmt.Printf("GetInfo err:%+v\n", errPa)
		err = errPa
		logger.Info("listAllParts err:", err)
		logger.Info(logger.TAIHETagMethodOut, "GetInfo")
		return info, err
	}


	offset := int64(0)

	for _, part := range parts {
		offset += int64(part.Size)
	}
	logger.Info("parts count:", len(parts))
	if parts == nil && offset == 0 {
		//不存在的情况下
		// fmt.Printf("parts is nil and offset=0\n")
		logger.Info("parts is nol and offset=0")
		info.Offset = info.Size
	} else {
		// fmt.Printf("offset=%d\n", offset)
		logger.Info("offset=", offset)
		info.Offset = offset
	}

	logger.Info(logger.TAIHETagMethodOut, "GetInfo")
	return
}

// func (store BosStore) getInfoLocal(fileName string) (tusd.FileInfo, error) {
//  info := tusd.FileInfo{}
//  data, err := ioutil.ReadFile(fileName + ".info")
//  if err != nil {
//      return info, err
//  }
//  if err := json.Unmarshal(data, &info); err != nil {
//      return info, err
//  }

//  stat, err := os.Stat(fileName)
//  if err != nil {
//      return info, err
//  }

//  info.Offset = stat.Size()

//  return info, nil
// }
func (store BosStore) GetReader(id, pid string) (io.Reader, error) {
	logger.Info(logger.TAIHETagMethodIn, "GetReader")
	uploadId, _ := splitIds(id)

	//获取BOS数据到流中
	objectKey := uploadId
	res, err := store.GetObject(objectKey, pid)
	if err == nil {
		logger.Info(logger.TAIHETagMethodOut, "GetReader")
		return res.Body, nil
	}

	// fmt.Printf("get Object failed err:%s\n", err)
	logger.Warn("get Object failed err:", err)
	return nil, err
	// If the file cannot be found, we ignore this error and continue since the
	// upload may not have been finished yet. In this case we do not want to
	// return a ErrNotFound but a more meaning-full message.
	// _, err = store.listAllParts(id)
	// if err != nil {
	//  fmt.Printf("GetReader err:%+v\n", err)
	//  return nil, err
	// }
	//从bos拉取文件存储到本地
	// return nil, err
}

func (store BosStore) Terminate(id, pid string) error {
	// fmt.Printf("Terminate\n")
	logger.Info(logger.TAIHETagMethodIn, "Terminate")
	UploadId, multipartId := splitIds(id)
	var wg sync.WaitGroup
	wg.Add(2)
	errs := make([]error, 0, 3)

	defer func(){
		if err := recover(); err != nil {
			logger.Warn("panic fetch:", err)
		}
	}()


	go func() {
		defer wg.Done()

		//Abort the multipart upload
		objectKey := UploadId
		uploadIdArgs := multipartId
		err := store.AbortMultipartUpload(objectKey,pid,  uploadIdArgs)
		if err != nil {
			errs = append(errs, err)
		}
	}()

	go func() {
		defer wg.Done()

		bosClient, err := store.getBosClient()
		if err != nil {
			// fmt.Printf("getBosClient err:%+v\n", err)
			logger.Warn("getBosClient err:", err)
			errs = append(errs, err)
			return
		}

		// Delete the info and content file
		deleteObjects := []string{UploadId, UploadId + ".info"}
		// bucketName := store.BucketName
		bucketName, err := store.getBucketName(pid)
		if err != nil {
			logger.Warn("getBucketName failed:", err)
			logger.Info(logger.TAIHETagMethodOut, "Terminate")
			errs = append(errs, err)
			return
		}

		res, err := bosClient.DeleteMultipleObjectsFromKeyList(bucketName, deleteObjects)
		if res != nil {
			//delete failed list
			logger.Warn("delete failed list")
		}
		if err != nil {
			// fmt.Printf("DeleteMultipleObjectsFromKeyList err:%+v\n", err)
			logger.Warn("DeleteMultipleObjectsFromKeyList err:", err)
			if err != io.EOF {
				errs = append(errs, err)
			}
			return
		}
	}()

	wg.Wait()

	if len(errs) > 0 {
		// fmt.Printf("%+v\n", errs)
		logger.Warn("err occurred")
		logger.Info(logger.TAIHETagMethodOut, "Terminate")
		return newMultiError(errs)
	}

	logger.Info(logger.TAIHETagMethodOut, "Terminate")
	return nil
}

func (store BosStore) FinishUpload(id, pid string) error {
	// fmt.Printf("FinishUpload\n")
	logger.Info(logger.TAIHETagMethodIn, "FinishUpload")
	bosClient, err := store.getBosClient()
	if err != nil {
		logger.Warn("getBosClient failed err:", err)
		logger.Info(logger.TAIHETagMethodOut, "FinishUpload")
		return err
	}

	uploadId, multipartId := splitIds(id)

	// Get uploaded parts
	parts, err := store.listAllParts(id, pid)
	if err != nil {
		// fmt.Printf("FinishUpload err:%+v\n", err)
		logger.Warn("listAllParts failed err:", err)
		logger.Info(logger.TAIHETagMethodOut, "FinishUpload")
		return err
	}

	partEtags := make([]api.UploadInfoType, len(parts))
	for index, part := range parts {
		partEtags[index] = api.UploadInfoType{
			PartNumber:     part.PartNumber,
			ETag:           part.ETag,
		}
	}

	completeArgs := &api.CompleteMultipartUploadArgs{partEtags}
	// bucketName := store.BucketName
	bucketName, err := store.getBucketName(pid)
	if err != nil {
		logger.Warn("getBucketName failed:", err)
		logger.Info(logger.TAIHETagMethodOut, "FinishUpload")
		return err
	}

	objectKey := uploadId
	uploadIdArgs := multipartId
	logger.Info("objectKey:", objectKey, "\tuploadIdArgs:", uploadIdArgs)
	bosClient.CompleteMultipartUploadFromStruct(bucketName, objectKey, uploadIdArgs, completeArgs, nil)

	logger.Info(logger.TAIHETagMethodOut, "FinishUpload")
	return nil
}

func (store BosStore) ConcatUploads(dest, pid string, partialUploads []string) error {
	logger.Info(logger.TAIHETagMethodIn, "ConcatUploads")
	uploadId, multipartId := splitIds(dest)

	numPartialUploads := len(partialUploads)
	errs := make([]error, 0, numPartialUploads)
	// Copy partial uploads concurrently
	var wg sync.WaitGroup
	wg.Add(numPartialUploads)
	defer func(){
		if err := recover(); err != nil {
			logger.Warn("panic fetch:", err)
		}
	}()

	for i, partialId := range partialUploads {
		go func(i int, partialId string) {
			defer wg.Done()

			partialUploadId, _ := splitIds(partialId)

			bucketName, err := store.getBucketName(pid)
			if err != nil {
				logger.Warn("getBucketName failed:", err)
				logger.Info(logger.TAIHETagMethodOut, "ConcatUploads")
				errs = append(errs, err)
				return
			}

			srcBucketName := bucketName
			srcObjectName := partialUploadId
			destBucketName := bucketName
			destObjectName := uploadId
			partNumber :=  i
			uploadIdArgs := multipartId
			_, err = store.UploadPartCopy(destBucketName, destObjectName, srcBucketName, srcObjectName, uploadIdArgs, partNumber)
			if err != nil {
				errs = append(errs, err)
				return
			}
		}(i, partialId)
	}

	wg.Wait()

	if len(errs) > 0 {
		logger.Warn("something error")
		logger.Info(logger.TAIHETagMethodOut, "ConcatUploads")
		return newMultiError(errs)
	}

	logger.Info(logger.TAIHETagMethodOut, "ConcatUploads")
	return store.FinishUpload(dest, pid)
}

func (store BosStore) UploadPartCopy(bucket, object, srcBucket, srcObject, uploadId string, partNumber int) (*api.CopyObjectResult, error) {
	logger.Info(logger.TAIHETagMethodIn, "UploadPartCopy")
	bosClient, err := store.getBosClient()
	if err != nil {
		logger.Warn("getBosClient failed err:", err)
		logger.Info(logger.TAIHETagMethodOut, "UploadPartCopy")
		return nil, err
	}

	logger.Info(logger.TAIHETagMethodOut, "UploadPartCopy")
	return bosClient.UploadPartCopy(bucket, object, srcBucket, srcObject, uploadId, partNumber, nil)
}


func (store BosStore) GetObject(objectKey, pid string) (*api.GetObjectResult, error) {
	logger.Info(logger.TAIHETagMethodIn, "GetObject")
	bosClient, err := store.getBosClient()
	if err != nil {
		logger.Warn("getBosClient failed err:", err)
		logger.Info(logger.TAIHETagMethodOut, "GetObject")
		return nil, err
	}

	bucketName, err := store.getBucketName(pid)
	if err != nil {
		logger.Warn("getBucketName failed:", err)
		logger.Info(logger.TAIHETagMethodOut, "GetObject")
		return nil, err
	}
	// bucketName := store.BucketName
	objectName := objectKey
	res, err := bosClient.BasicGetObject(bucketName, objectName)

	logger.Info(logger.TAIHETagMethodOut, "GetObject")
	return res, err
}

func (store BosStore) getBosClient() (*bos.Client, error) {
	logger.Info(logger.TAIHETagMethodIn, "getBosClient")
	ak := store.AccessKeyID
	sk := store.SecretAccessKey
	endpoint := store.Endpoint

	bosClient, err := bos.NewClient(ak, sk, endpoint)
	// 配置连接超时时间为30秒
	bosClient.Config.ConnectionTimeoutInMillis = 30 * 1000

	logger.Info(logger.TAIHETagMethodOut, "getBosClient")
	return bosClient, err
}

func (store BosStore) listAllParts(id, pid string) ([]api.ListPartType, error) {
	// fmt.Printf("listAllParts\n")
	logger.Info(logger.TAIHETagMethodIn, "listAllParts")
	bosClient, err := store.getBosClient()
	if err != nil {
		logger.Warn("getBosClient failed err:", err)
		logger.Info(logger.TAIHETagMethodOut, "listAllParts")
		return nil, err
	}
	uploadId, multipartId := splitIds(id)
	args := new(api.ListPartsArgs)
	args.MaxParts = 1000
	objectKey := uploadId
	uploadIdArgs := multipartId 

	bucketName, err := store.getBucketName(pid)
	if err != nil {
		logger.Warn("getBucketName failed:", err)
		logger.Info(logger.TAIHETagMethodOut, "listAllParts")
		return nil, err
	}
	parts, err := bosClient.ListParts(bucketName, objectKey, uploadIdArgs, args)
	if err != nil {
		// fmt.Printf("listAllParts objectKey:%+v\tuploadIdArgs:%+v\terr:%+v\n", objectKey, uploadIdArgs, err)
		logger.Warn("listAllParts objectKey:", objectKey, "\tuploadIdArgs:", uploadIdArgs, "\terr:", err)
		errBceServiceError := err.(*bce.BceServiceError)
		if errBceServiceError.Code == "NoSuchUpload" {
			// fmt.Printf("NoSuchUpload Wow\n")
			logger.Warn("NoSuchUpload Wow")
			return nil, nil
		}

		logger.Info(logger.TAIHETagMethodOut, "listAllParts")
		return nil, err
	}

	logger.Info(logger.TAIHETagMethodOut, "listAllParts")
	return parts.Parts, err
}

/*获取最佳分块大小*/
func (store BosStore) calcOptimalPartSize(size int64) (optimalPartSize int64, err error) {
	switch {
		// When upload is smaller or equal MinPartSize, we upload in just one part.
		case size <= store.MinPartSize:
		optimalPartSize = store.MinPartSize
		// Does the upload fit in MaxMultipartParts parts or less with MinPartSize.
		case size <= store.MinPartSize*store.MaxMultipartParts:
			optimalPartSize = store.MinPartSize
		// Prerequisite: Be aware, that the result of an integer division (x/y) is
		// ALWAYS rounded DOWN, as there are no digits behind the comma.
		// In order to find out, whether we have an exact result or a rounded down
		// one, we can check, whether the remainder of that division is 0 (x%y == 0).
		//
		// So if the result of (size/MaxMultipartParts) is not a rounded down value,
		// then we can use it as our optimalPartSize. But if this division produces a
		// remainder, we have to round up the result by adding +1. Otherwise our
		// upload would not fit into MaxMultipartParts number of parts with that
		// size. We would need an additional part in order to upload everything.
		// While in almost all cases, we could skip the check for the remainder and
		// just add +1 to every result, but there is one case, where doing that would
		// doom our upload. When (MaxObjectSize == MaxPartSize * MaxMultipartParts),
		// by adding +1, we would end up with an optimalPartSize > MaxPartSize.
		// With the current S3 API specifications, we will not run into this problem,
		// but these specs are subject to change, and there are other stores as well,
		// which are implementing the S3 API (e.g. RIAK, Ceph RadosGW), but might
		// have different settings.
		case size%store.MaxMultipartParts == 0:
			optimalPartSize = size / store.MaxMultipartParts
		// Having a remainder larger than 0 means, the float result would have
		// digits after the comma (e.g. be something like 10.9). As a result, we can
		// only squeeze our upload into MaxMultipartParts parts, if we rounded UP
		// this division's result. That is what is happending here. We round up by
		// adding +1, if the prior test for (remainder == 0) did not succeed.
		default:
			optimalPartSize = size/store.MaxMultipartParts + 1
	}

	// optimalPartSize must never exceed MaxPartSize
	if optimalPartSize > store.MaxPartSize {
		return optimalPartSize, fmt.Errorf("calcOptimalPartSize: to upload %v bytes optimalPartSize %v must exceed MaxPartSize %v", size, optimalPartSize, store.MaxPartSize)
	}
	return optimalPartSize, nil
}

func (store BosStore) UploadPart(file *os.File, objectKey, pid, uploadIdArgs string, partNumber int64, bosClient *bos.Client) (err error) {
	logger.Info(logger.TAIHETagMethodIn, "UploadPart")

	// 创建指定偏移、指定大小的文件流
	offset := int64(0)
	fileInfo, _ := file.Stat()
	fileSize := fileInfo.Size()
	uploadSize := fileSize
	partBody, errNewBody := bce.NewBodyFromSectionFile(file, offset, uploadSize)
	if errNewBody != nil {
		err = errNewBody
		logger.Warn("NewBodyFromSectionFile failed err:", err)
		logger.Info(logger.TAIHETagMethodOut, "UploadPart")
		return
	}

	contentMD5 := partBody.ContentMD5()
	partSize := partBody.Size()
	logger.Info("partBody info: size:", partSize, "\tcontentMD5:", contentMD5)

	// 上传当前分块
	bucketName, err := store.getBucketName(pid)
	if err != nil {
		logger.Warn("getBucketName failed:", err)
		logger.Info(logger.TAIHETagMethodOut, "UploadPart")
		return err
	}
	// bucketName := bucketName
	uploadId := uploadIdArgs

	//当前上传限流起始点
	beginTimestamp := time.Now().UnixNano()
	//当前限流结束点
	defer func(beginTimestampm, uploadSize int64){
		defer func(){
			if err := recover(); err != nil {
				logger.Warn("panic fetch:", err)
			}
		}()
		endTimestamp := time.Now().UnixNano()
		paseDuration := (endTimestamp - beginTimestamp) / 1000000
		uploadSpeed := ((uploadSize >> 20) * 1000) / paseDuration // MB/s
		logger.Info("uploadSize:", uploadSize, "\tpaseDuration:", paseDuration, "\tuploadSpeed:", uploadSpeed)
		if uploadSpeed > 1 {
			rand.Seed(time.Now().UnixNano())
			paseDuration := rand.Intn(100) + 50 //[50, 150)
			logger.Warn("uploadSpeed too fast current speed:", uploadSpeed, " MB/s will pase ", paseDuration, " ms")
			time.Sleep(time.Duration(paseDuration) * time.Millisecond)
		} 
	}(beginTimestamp, uploadSize)

	_, err = bosClient.BasicUploadPart(bucketName, objectKey, uploadId, int(partNumber), partBody)

	//校验文件内容的md5值
	fileMD5 := partBody.ContentMD5()
	if fileMD5 != contentMD5 {
		logger.Warn("check md5sum failed src fileMD5:", fileMD5, "\tpart MD5:", contentMD5)
		logger.Info(logger.TAIHETagMethodOut, "UploadPart")
		return errors.New("checksum failed")
	}

	logger.Info(logger.TAIHETagMethodOut, "UploadPart")
	return err
}

func (store BosStore) GeturlAddr(id, pid string, offset int64) (string, error){
	logger.Info(logger.TAIHETagMethodIn, "GeturlAddr")
	bosClient, err := store.getBosClient()
	if err != nil {
		logger.Warn("getBosClient failed err:", err)
		logger.Info(logger.TAIHETagMethodOut, "GeturlAddr")
		return "", err
	}

	uploadId, _ := splitIds(id)

	expirationInSeconds := -1
	method := "GET"
	headers := map[string]string {
		"headers": store.Endpoint,
	}
	authParams := sin(expirationInSeconds, store.AccessKeyID)
	params := map[string]string {
		"authorization":authParams,
	}
	objectName := uploadId

	bucketName, err := store.getBucketName(pid)
	if err != nil {
		logger.Warn("getBucketName failed:", err)
		logger.Info(logger.TAIHETagMethodOut, "GeturlAddr")
		return "", err
	}

	url := bosClient.GeneratePresignedUrl(bucketName, objectName,
		expirationInSeconds, method, headers, params)
	//添加域名替换
	store.writeBosUrl(id, pid, url, offset)

	logger.Info(logger.TAIHETagMethodOut, "GeturlAddr")
	return url, nil
}

func (store BosStore) AbortMultipartUpload(objectKey, pid, uploadId string) error {
	logger.Info(logger.TAIHETagMethodIn, "AbortMultipartUpload'")
	bosClient, err := store.getBosClient()
	if err != nil {
		logger.Warn("getBosClient failed err:", err)
		logger.Info(logger.TAIHETagMethodOut, "AbortMultipartUpload")
		return err
	}

	bucketName, err := store.getBucketName(pid)
	if err != nil {
		logger.Warn("getBucketName failed:", err)
		logger.Info(logger.TAIHETagMethodOut, "AbortMultipartUpload")
		return err
	}

	bosClient.AbortMultipartUpload(bucketName, objectKey, uploadId)

	logger.Info(logger.TAIHETagMethodOut, "AbortMultipartUpload")
	return nil
}

func (store BosStore) DeclareLength(id, pid string, length int64) error {
	logger.Info(logger.TAIHETagMethodIn, "DeclareLength")
	info, err := store.GetInfo(id, pid)
	if err != nil {
		logger.Warn("GetInfo failed err:", err)
		logger.Info(logger.TAIHETagMethodOut, "DeclareLength")
		return err
	}
	info.Size = length
	info.SizeIsDeferred = false

	logger.Info(logger.TAIHETagMethodOut, "DeclareLength")
	return store.writeInfo(id, info)
}

// writeInfo updates the entire information. Everything will be overwritten.
func (store BosStore) writeInfo(id string, info tusd.FileInfo) error {
	// fmt.Printf("uploda bos info id%+v\t:%+v\n", id, info)
	logger.Info(logger.TAIHETagMethodIn, "writeInfo")
	logger.Info("uploda bos info. info information: info.ID:", info.ID, "\tinfo.Size:", info.Size, "\tinfo.Offset:", info.Offset, "\tinfo.IsPartial:", info.IsPartial,
		"\tinfo.IsFinal:", info.IsFinal, "\tPartialUploads len:", len(info.PartialUploads))
	infoJson, err := json.Marshal(info)
	if err != nil {
		logger.Warn("Marshal failed err:", err)
		logger.Info(logger.TAIHETagMethodOut, "writeInfo")
		return err
	}

	uploadId, _ := splitIds(id)
	// Create object on bos containing information about the file
	objectKey := uploadId + ".info"
	pid := info.MetaData["pid"]
	_, err = store.PutObject(objectKey, pid, infoJson)
	if err != nil {
		logger.Warn("PutObject failed err:", err)
		logger.Info(logger.TAIHETagMethodOut, "writeInfo")
		return err
	}

	logger.Info(logger.TAIHETagMethodOut, "writeInfo")
	return nil
}

// // writeInfo updates the entire information. Everything will be overwritten.
// func (store BosStore) writeInfoLocal(fileName string, info tusd.FileInfo) error {
//  data, err := json.Marshal(info)
//  if err != nil {
//      return err
//  }
//  fmt.Printf("write info file:%s\n", fileName + ".info")
//  return ioutil.WriteFile(fileName + ".info", data, defaultFilePerm)
// }


func (store BosStore) writeBosUrl(id, pid string, bosurl string, offset int64) error {
	logger.Info(logger.TAIHETagMethodIn, "writeBosUrl")
	info, err := store.GetInfo(id, pid)
	if err != nil {
		logger.Warn("GetInfo failed err:", err)
		logger.Info(logger.TAIHETagMethodOut, "writeBosUrl")
		return err
	}

	info.MetaData = map[string]string{"fileurl":bosurl}
	info.Offset = offset
	// fmt.Printf("writeBosUrl info:\n", info)
	logger.Info("writeBosUrl info. information: info.ID:", info.ID, "\tinfo.Size:", info.Size, "\tinfo.Offset:", info.Offset, "\tinfo.IsPartial:",
	 info.IsPartial,"\tinfo.IsFinal:", info.IsFinal, "\tPartialUploads len:", len(info.PartialUploads))

	logger.Info(logger.TAIHETagMethodOut, "writeBosUrl")
	return store.writeInfo(id, info)
}


func (store BosStore) getBucketName(pid string) (string, error) {
	if _, ok := store.BucketNames[pid]; ! ok {
		logger.Warn("pid:", pid, " invalid")
		return "", errors.New("pid invalid")
	}

	bucketName := store.BucketNames[pid]
	logger.Info("pid=>bucketName: ", pid, "=>", bucketName)
	return bucketName, nil
}
////////////////////commom tool//////////////////////////////
func RandomStr(length int) string {
	str := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	bytes := []byte(str)
	result := []byte{}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < length; i++ {
		result = append(result, bytes[r.Intn(len(bytes))])
	}
	return string(result)
}

func sin(expirationInSeconds int, ak string) (auth string) {
	expirStr := strconv.Itoa(expirationInSeconds)
	tm := time.Unix(time.Now().Unix(), 0)
	timeStr := tm.Format("2006-01-02")
	h := sha256.New()
	h.Write([]byte( RandomStr(64) + ak))
	sha256SigningKey :=  string(h.Sum(nil))
	auth = "bce-auth-v1/" + ak + "/" + timeStr + "/" + expirStr + "//" + sha256SigningKey
	return
}

func splitIds(id string) (uploadId, multipartId string) {
	index := strings.Index(id, "+")
	if index == -1 {
		return
	}

	uploadId = id[:index]
	multipartId = id[index+1:]
	return
}

func newMultiError(errs []error) error {
	message := "Multiple errors occurred:\n"
	for _, err := range errs {
		message += "\t" + err.Error() + "\n"
	}
	return errors.New(message)
}