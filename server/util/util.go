package util

import(
	"github.com/config"
	"github.com/tus/tusd"
	"github.com/tus/tusd/bosstore"
)

type HttpConfig struct {
	Addrs string
}

///////////////////////objs/////////////////////////////
func GetHttpConfig(cfg config.Configer) (HttpConfig, error) {
	sec, err := cfg.GetSection("http")
	if err != nil {
		return HttpConfig{}, err
	}
	bindAddr := sec.GetStringMust("bindAddr", "0.0.0.0:8088")
	return HttpConfig{Addrs: bindAddr,}, nil
}

func GetUploadStore(cfg config.Configer) (bosstore.BosStore, error) {
	sec, err := cfg.GetSection("upload")
	if err != nil {
		return bosstore.BosStore{}, nil
	}
	accessKeyID := sec.GetStringMust("accessKeyID", "123456")
	secretAccessKey := sec.GetStringMust("secretAccessKey", "123456")
	endpoint := sec.GetStringMust("endpoint", "music.taihe.com")
	// bucketName := sec.GetStringMust("bucketName", "ertongxingxuan")
	secBuckNames, err  := cfg.GetSection("upload-bucketnames")
	if err != nil {
		return bosstore.BosStore{}, nil
	}

	bucketNames := secBuckNames.MapTo()
	bucketNames[""] = "ertongxingxuan"
	// fmt.Printf("%+v\n", bucketNames)
	// os.Exit(1)

	store := bosstore.New(accessKeyID, secretAccessKey, endpoint, bucketNames)
	return store, nil
}

func GetTusdConfig(cfg config.Configer) (tusd.Config, error) {
	sec, err := cfg.GetSection("tusd")
	if err != nil {
		return tusd.Config{}, err
	}
	basePath := sec.GetStringMust("basePath", "/files/")
	maxSize := sec.GetIntMust("maxSize", 1024 * 1024 * 1024)

	tusdConfig := tusd.Config{//具体的请参考Config的结构体
		BasePath:  basePath,
		MaxSize: maxSize,
	}
	return tusdConfig, err
}