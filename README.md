![CLOJURE](https://clojure.org/images/clojure-logo-120b.png)

# kafka-simple-test
Just a kafka sandbox in clojure.

### Starting a local Kafka Server
At this project root directorie, type:
```bash
sudo docker-compose up
```
> You will need to install [docker](http://www.docker.com) and [docker-compose](https://docs.docker.com/compose/install/)

### Start the consumer service
```bash
lein consume
```

### Produce messages

```bash
lein produce
```
