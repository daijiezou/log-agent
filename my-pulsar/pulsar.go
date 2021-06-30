package my_pulsar

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/sirupsen/logrus"
	"log_collection/common"
	"log_collection/my-pulsar/pulsar_msg_model"
	"time"
)

var (
	Client   pulsar.Client
	producer pulsar.Producer
	msgChan  chan *pulsar_msg_model.PulsarMsg
	pMgr     *pulsarTaskMgr
)

func Init(address string, allConf []common.CollectEntry) (err error) {
	Client, err = pulsar.NewClient(pulsar.ClientOptions{
		URL:               address,
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		logrus.Fatalf("Could not instantiate Pulsar Client: %v", err)
	}
	pMgr = &pulsarTaskMgr{
		pulsarTaskMap:    make(map[string]*pulsarTask, 20),
		collectEntryList: allConf,
		confChan:         make(chan []common.CollectEntry), // 做一个阻塞channel
	}
	for _, v := range allConf {
		producer, err := Client.CreateProducer(pulsar.ProducerOptions{
			Topic: v.Topic,
		})
		if err != nil {
			logrus.Errorf("create producer for topic:%s failed, err:%v", v.Topic, err)
			continue
		}
		fmt.Println(v.Topic)
		pt := newPulsarProducer(producer)
		pMgr.pulsarTaskMap[v.Topic] = pt
		logrus.Infof("create producer for topic:%s succeed", v.Topic)
	}
	// todo 待优化
	msgChan = make(chan *pulsar_msg_model.PulsarMsg, 100)
	go sendMsg()
	go pMgr.Watch()
	return
}

func newPulsarProducer(p pulsar.Producer) *pulsarTask {
	ctx, cancel := context.WithCancel(context.Background())
	pt := pulsarTask{
		pObj:   p,
		ctx:    ctx,
		cancel: cancel,
	}
	return &pt
}

// 从MsgChan中读取msg,发送给kafka
func sendMsg() {
	for {
		select {
		case msg := <-msgChan:
			messageId, err := pMgr.pulsarTaskMap[msg.Topic].pObj.Send(context.Background(), &pulsar.ProducerMessage{
				Payload: []byte(msg.Msg),
			})
			if err != nil {
				logrus.Warning("send msg failed, err:", err)
			}
			logrus.Infof("send msg to pulsar success. messageId:%v ", messageId)
		}
	}
}

// 定义一个函数向外暴露msgChan
func ToMsgChan(msg *pulsar_msg_model.PulsarMsg) {
	msgChan <- msg
}
