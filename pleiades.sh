#!/bin/sh

java -Xms1000M -Xmx4000M -Dhazelcast.logging.type=none -jar Pleiades-0.1.jar $@
