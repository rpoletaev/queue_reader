package queue_reader

import (
	"time"

	"github.com/garyburd/redigo/redis"
	mgo "gopkg.in/mgo.v2"
)

func (svc *service) setupMongo() {
	mongo, err := mgo.Dial(svc.Mongo)
	if err != nil {
		panic(err)
	}

	svc.mongo = mongo
	svc.mongo.SetMode(mgo.Monotonic, true)
}

func (svc *service) mongoExec(colectionName string, execFunc func(*mgo.Collection) error) error {
	session := svc.mongo.Clone()
	defer session.Close()

	db := session.DB(svc.MongoDB)
	collection := db.C(colectionName)
	return execFunc(collection)
}

func newRedisPool(addr, pwd string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", addr, redis.DialDatabase(0), redis.DialPassword(pwd))
		},
	}
}

func (svc *service) ClearQueue() error {
	conn := svc.redisPool.Get()
	defer conn.Close()
	_, err := conn.Do("DEL", "FileQueue")
	return err
}

func (svc *service) ClearErrorQueue() (removed int, err error) {
	svc.mongoExec(svc.ErrorCollection, func(c *mgo.Collection) error {
		i, err := c.RemoveAll(nil)
		if err != nil {
			return err
		}

		removed = i.Removed
		return nil
	})
	return removed, err
}
