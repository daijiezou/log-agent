package main

import (
	"fmt"
	"log_collection/common"
	"log_collection/etcd"
	my_pulsar "log_collection/my-pulsar"
	"log_collection/tailfile"

	"github.com/sirupsen/logrus"
	"gopkg.in/ini.v1"
)

// 整个logagent的配置结构体
type Config struct {
	KafkaConfig `ini:"kafka"`
	EtcdConfig  `ini:"etcd"`
	Pulsar      `ini:"pulsar"`
}
type KafkaConfig struct {
	Address  string `ini:"address"`
	ChanSize int64  `ini:"chan_size"`
}

type EtcdConfig struct {
	Address    string `ini:"address"`
	CollectKey string `ini:"collect_key"`
}

type Pulsar struct {
	Address string `ini:"address"`
}

func main() {
	// 获取本机ip
	ip, err := common.GetOutboundIP()
	if err != nil {
		logrus.Error("Get OutboundIP failed,err:%v", err)
		return
	}
	fmt.Println("测试github。。。。")
	var confObj = new(Config)
	// 从ini中读取静态的配置文件
	err = ini.MapTo(confObj, "./conf/config.ini")
	if err != nil {
		logrus.Error("load conf failed,err:%v", err)
		return
	}
	// 初始化
	/*	err = kafka.Init([]string{confObj.KafkaConfig.Address}, confObj.ChanSize)
		if err != nil {
			logrus.Errorf("init kafka failed,err:%v", err.Error())
			return
		}*/
	//初始化etcd
	err = etcd.Init([]string{confObj.EtcdConfig.Address})
	if err != nil {
		logrus.Errorf("init etcd failed,err:%v", err.Error())
		return
	}
	// 从etcd中拉取要收集日志的配置项。每个机器上拉取的日志的地址可能不同，通过ip来设置每个机器拉取配置的key
	collectKey := fmt.Sprintf(confObj.EtcdConfig.CollectKey, ip)
	allConf, err := etcd.GetConf(collectKey)
	if err != nil {
		logrus.Errorf("tcd.GetConf failed,err:%v", err.Error())
		return
	}
	fmt.Println("allConf", allConf)
	// 监控etcd中 confObj.EtcdConfig.CollectKey 对应值的变换
	go etcd.WatchConf(collectKey)
	// 把从etcd中获取的配置项,
	err = tailfile.Init(allConf)
	if err != nil {
		logrus.Errorf("init tailFile failed,err:%v", err.Error())
		return
	}
	// 初始化pulsar
	err = my_pulsar.Init(confObj.Pulsar.Address, allConf)
	if err != nil {
		logrus.Errorf("init pulsar failed,err:%v", err.Error())
		return
	}
	logrus.Info("服务初始化成功!!!")
	select {}
}
