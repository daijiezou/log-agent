package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"log_collection/common"
	my_pulsar "log_collection/my-pulsar"
	"log_collection/tailfile"
	"time"

	"github.com/sirupsen/logrus"

	"go.etcd.io/etcd/client/v3"
)

var (
	client *clientv3.Client
)

//etcd 相关操作
func Init(addr []string) (err error) {
	client, err = clientv3.New(clientv3.Config{
		Endpoints:  addr,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		// handle error!
		fmt.Printf("connect to etcd failed, err:%v\n", err)
		return
	}
	return nil
}

// GetConf 拉取日志收集配置项目的函数
func GetConf(key string) (collectEntry []common.CollectEntry, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	resp, err := client.Get(ctx, key)
	if err != nil {
		logrus.Errorf("get conf from etcd by key:%s failed,err:%v", key, err.Error())
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		logrus.Warnf("get len:0 conf from etcd by key:%s", key)
		return nil, err
	}
	ret := resp.Kvs[0]
	err = json.Unmarshal(ret.Value, &collectEntry)
	if err != nil {
		logrus.Errorf("json.Unmarshal failed,err:%v", err.Error())
	}
	return
}

// WatchConf 监控日志收集项目配置变化的项目
func WatchConf(key string) {
	rch := client.Watch(context.Background(), key)
	newConf := new([]common.CollectEntry)
	for wresp := range rch {
		for _, ev := range wresp.Events {
			fmt.Printf("Type: %s Key:%s Value:%s\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
			if ev.Type == clientv3.EventTypeDelete {
				// 如果是删除
				logrus.Warning("FBI warning:etcd delete the key!!!")
				tailfile.SendNewConf(*newConf) // 没有任何接收就是阻塞的
				continue
			}
			err := json.Unmarshal(ev.Kv.Value, newConf)
			if err != nil {
				logrus.Errorf("json.Unmarshal new conf failed,err:%v", err)
				continue
			}
			my_pulsar.SendNewConf(*newConf)
			time.Sleep(time.Second)
			// 告诉tailfile这个模块应该使用新的配置了
			tailfile.SendNewConf(*newConf) // 没有任何接收就是阻塞的

		}
	}
}
