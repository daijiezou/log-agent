package tailfile

import (
	"log_collection/common"

	"github.com/sirupsen/logrus"
)

// tailTask 的管理者

type tailTaskMgr struct {
	tailTaskMap      map[string]*tailTask       // 所有的tailTask任务
	collectEntryList []common.CollectEntry      // 所有配置项
	confChan         chan []common.CollectEntry // 等待新配置的通道
}

var (
	ttMgr *tailTaskMgr
)

// main函数中调用
func Init(allConf []common.CollectEntry) (err error) {
	// allConf里面存了若干个日志的收集项,
	// 针对每一个日志收集项创建一个对应的tailObj
	ttMgr = &tailTaskMgr{
		tailTaskMap:      make(map[string]*tailTask, 20),
		collectEntryList: allConf,
		confChan:         make(chan []common.CollectEntry), // 做一个阻塞channel
	}
	for _, conf := range allConf {
		tt := newTailTask(conf.Path, conf.Topic) // 创建一个日志收集任务
		err = tt.Init()                          // 去打开日志文件准备读
		if err != nil {
			logrus.Errorf("create tailObj for path:%s failed, err:%v", conf.Path, err)
			continue
		}
		logrus.Infof("create a tail task for path:%s success", conf.Path)
		ttMgr.tailTaskMap[tt.path] = tt // 把创建的这个tailTask任务登记在册,方便后续管理
		// 起一个后台的goroutine去收集日志
		go tt.Run()
	}
	// 在后台等新的配置来
	go ttMgr.watch()
	return
}

func (t *tailTaskMgr) watch() {
	for {
		// 派一个小弟等着新配置来,
		newConf := <-t.confChan // 取到值说明新的配置来啦
		// 新配置来了之后应该管理一下我之前启动的那些tailTask
		logrus.Infof("get new conf from etcd, conf:%v, start manage tailTask...", newConf)
		for _, conf := range newConf {
			// 1. 原来已经存在的任务就不用动
			if t.isExist(conf) {
				continue
			}
			// 2. 原来没有的我要新创建一个taiTask任务
			tt := newTailTask(conf.Path, conf.Topic) // 创建一个日志收集任务
			err := tt.Init()                         // 去打开日志文件准备读
			if err != nil {
				logrus.Errorf("create tailObj for path:%s failed, err:%v", conf.Path, err)
				continue
			}
			logrus.Infof("create a tail tailTask for path:%s success", conf.Path)
			t.tailTaskMap[tt.path] = tt // 把创建的这个tailTask任务登记在册,方便后续管理
			// 起一个后台的goroutine去收集日志
			go tt.Run()
		}
		// 3. 原来有的现在没有的要tailTask停掉
		// 找出tailTaskMap中存在,但是newConf不存在的那些tailTask,把它们都停掉
		for key, tailTask := range t.tailTaskMap {
			var found bool
			for _, conf := range newConf {
				if key == conf.Path {
					found = true
					break
				}
			}
			if !found {
				// 这个tailTask要停掉了
				logrus.Infof("the tailTask collect path:%s need to stop.", tailTask.path)
				delete(t.tailTaskMap, key) // 从管理类中删掉
				tailTask.cancel()
			}
		}
	}
}

// 判断tailTaskMap中是否存在该收集项
func (t *tailTaskMgr) isExist(conf common.CollectEntry) bool {
	_, ok := t.tailTaskMap[conf.Path]
	return ok
}

func SendNewConf(newConf []common.CollectEntry) {
	ttMgr.confChan <- newConf
}
