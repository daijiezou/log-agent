package tailfile

import (
	"context"
	"fmt"
	my_pulsar "log_collection/my-pulsar"
	"log_collection/my-pulsar/pulsar_msg_model"
	"time"

	"github.com/hpcloud/tail"
	"github.com/sirupsen/logrus"
)

type tailTask struct {
	path   string
	topic  string
	tObj   *tail.Tail
	ctx    context.Context
	cancel context.CancelFunc
}



func newTailTask(path, topic string) *tailTask {
	ctx, cancel := context.WithCancel(context.Background())
	tt := tailTask{
		path:   path,
		topic:  topic,
		ctx:    ctx,
		cancel: cancel,
	}
	return &tt
}

func (t *tailTask) Init() (err error) {
	cfg := tail.Config{
		ReOpen:    true,
		Follow:    true,
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2},
		MustExist: false,
		Poll:      true,
	}
	t.tObj, err = tail.TailFile(t.path, cfg)
	return err
}

func (t *tailTask) Run() {
	// 读取日志,发往kafka
	logrus.Infof("collect for path:%s is running...", t.path)
	for {
		select {
		case <-t.ctx.Done(): // 只要调用t.cancel() 就会收到信号
			logrus.Infof("path:%s is stopping...", t.path)
			return
			// 循环读数据
		case line, ok := <-t.tObj.Lines: // chan tail.Line
			if !ok {
				logrus.Warn("tail file close reopen, path:%s\n", t.path)
				time.Sleep(time.Second) // 读取出错等一秒
				continue
			}
			// 如果是空行就略过
			//fmt.Printf("%#v\n", line.Text)
			if len(line.Text) == 0 {
				logrus.Info("出现空行拉,直接跳过...")
				continue
			}
			/*			// 利用通道将同步的代码改为异步的
						// 把读出来的一行日志包装成kafka里面的msg类型
						Msg := &sarama.ProducerMessage{}
						Msg.Topic = t.Topic // 每个tailObj自己的topic
						Msg.Value = sarama.StringEncoder(line.Text)
						// 丢到通道中
						kafka.ToMsgChan(Msg)*/
			pm := new(pulsar_msg_model.PulsarMsg)
			pm.Msg = line.Text
			pm.Topic = t.topic

			fmt.Println("pm:",pm)
			my_pulsar.ToMsgChan(pm)
		}
	}
}
