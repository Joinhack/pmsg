package pmsg

import (
	"github.com/garyburd/redigo/redis"
	"io"
	"strconv"
)

type RedisClientOfflineOfflineCenter struct {
	OfflineCenter
	offlineMsgFilters []OfflineMsgFilter
	pool              *redis.Pool
}

func NewRedisClientOfflineCenter(pool *redis.Pool) *RedisClientOfflineOfflineCenter {
	return &RedisClientOfflineOfflineCenter{pool: pool}
}

func (r *RedisClientOfflineOfflineCenter) AddOfflineMsgFilter(filter OfflineMsgFilter) {
	r.offlineMsgFilters = append(r.offlineMsgFilters, filter)
}

func (r *RedisClientOfflineOfflineCenter) ProcessMsg(msg RouteMsg) {
	msg.SetType(OfflineMsgType)
	
	for _, filter := range r.offlineMsgFilters {
		if !filter(msg) {
			return
		}
	}
	func() {
		defer func() {
			if e := recover(); e != nil {
				ERROR.Println(e)
			}
		}()
		conn := r.pool.Get()
		defer conn.Close()
		var err error
		to := strconv.FormatUint(uint64(msg.Destination()), 10)

		_, err = redis.Int(conn.Do("msg_append", to, msg.Body()))
		if err != nil {
			ERROR.Println(err)
			return
		}
	}()
}


func (r *RedisClientOfflineOfflineCenter) OfflineOutgoingPrepared(conn io.Writer) error {
	panic("can't happend in this implementation.")
}

func (r *RedisClientOfflineOfflineCenter) OfflineMsgReplay(id uint32) {
	//don't replay
}

func (r *RedisClientOfflineOfflineCenter) OfflineIncomingMsg(byte, io.Reader) error {
	panic("can't happend in this implementation.")
}

func (r *RedisClientOfflineOfflineCenter) OfflineIncomingControlMsg(byte, byte, io.Reader) error {
	panic("can't happend in this implementation.")
}

func (r *RedisClientOfflineOfflineCenter) Close() {
}
