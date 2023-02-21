import os

REDIS_CFG = {
	"host" : os.getenv('REDIS_HOST'),
	"port" : 6379,
	"password" : os.getenv('REDIS_PASS')
} 
