#!/bin/sh
HOST=omniwallet-test.ccbz9vycl90c.us-west-2.rds.amazonaws.com
dropdb --echo --if-exists --host=$HOST --port=5432 --username=xmcmaster omniwallet
