package my_pulsar

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/sirupsen/logrus"
	"log_collection/common"
)

type pulsarTask struct {
	pObj   pulsar.Producer
	ctx    context.Context
	cancel context.CancelFunc
}

type pulsarTaskMgr struct {
	pulsarTaskMap    map[string]*pulsarTask     // 所有的tailTask任务
	collectEntryList []common.CollectEntry      // 所有配置项
	confChan         chan []common.CollectEntry // 等待新配置的通道
}

func (p *pulsarTaskMgr) Watch() {
	for {
		// 派一个小弟等着新配置来,
		newConf := <-p.confChan // 取到值说明新的配置来啦
		// 新配置来了之后应该管理一下我之前启动的那些tailTask
		logrus.Infof("get new conf from etcd, conf:%v, start manage pulsarTask...", newConf)
		for _, conf := range newConf {
			// 1. 原来已经存在的任务就不用动
			if p.isExist(conf) {
				continue
			}
			// 2. 原来没有的我要新创建一个PulsarProducer
			producer, err := Client.CreateProducer(pulsar.ProducerOptions{
				Topic: conf.Topic,
			})
			if err != nil {
				logrus.Errorf("create producer for topic:%s failed, err:%v", conf.Topic, err)
				continue
			}
			pp := newPulsarProducer(producer)

			logrus.Infof("create a tail task for path:%s success", conf.Path)
			p.pulsarTaskMap[conf.Topic] = pp // 把创建的这个tailTask任务登记在册,方便后续管理
		}
		// 3. 原来有的现在没有的要PulsarProducer停掉
		for key, task := range p.pulsarTaskMap {
			var found bool
			for _, conf := range newConf {
				if key == conf.Topic {
					found = true
					break
				}
			}
			if !found {
				// 这个PulsarProducer要停掉了
				logrus.Infof("the PulsarProducer topic:%s need to stop.", task.pObj.Topic())
				task.pObj.Close()
				delete(p.pulsarTaskMap, key) // 从管理类中删掉
			}
		}
	}
}

// 判断pulsarTaskMgr中是否有该生产者
func (p *pulsarTaskMgr) isExist(conf common.CollectEntry) bool {
	_, ok := p.pulsarTaskMap[conf.Topic]
	return ok
}

func SendNewConf(newConf []common.CollectEntry) {
	pMgr.confChan <- newConf
}
