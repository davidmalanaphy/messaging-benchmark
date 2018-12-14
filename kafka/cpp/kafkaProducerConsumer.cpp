/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2014, Magnus Edenhill
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met: 
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer. 
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution. 
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE 
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF 
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * Apache Kafka consumer & producer example programs
 * using the Kafka driver from librdkafka
 * (https://github.com/edenhill/librdkafka)
 */

#include <iostream>
#include <string>
#include <cstdlib>
#include <cstdio>
#include <csignal>
#include <cstring>
#include <pthread.h>
#include <sys/time.h>
#include <sstream>

#ifdef _MSC_VER
#include "../win32/wingetopt.h"
#elif _AIX
#include <unistd.h>
#else
#include <getopt.h>
#endif

/*
 * Typically include path in a real application would be
 * #include <librdkafka/rdkafkacpp.h>
 */
#include "rdkafkacpp.h"

static bool run = true;
static bool exit_eof = false;

static void sigterm (int sig) {
  run = false;
}

class ExampleDeliveryReportCb : public RdKafka::DeliveryReportCb {
 public:
  void dr_cb (RdKafka::Message &message) {
    std::string status_name;
    switch (message.status())
      {
      case RdKafka::Message::MSG_STATUS_NOT_PERSISTED:
        status_name = "NotPersisted";
        break;
      case RdKafka::Message::MSG_STATUS_POSSIBLY_PERSISTED:
        status_name = "PossiblyPersisted";
        break;
      case RdKafka::Message::MSG_STATUS_PERSISTED:
        status_name = "Persisted";
        break;
      default:
        status_name = "Unknown?";
        break;
      }
    std::cout << "Message delivery for (" << message.len() << " bytes): " <<
      status_name << ": " << message.errstr() << std::endl;
    if (message.key())
      std::cout << "Key: " << *(message.key()) << ";" << std::endl;
  }
};


class ExampleEventCb : public RdKafka::EventCb {
 public:
  void event_cb (RdKafka::Event &event) {
    switch (event.type())
    {
      case RdKafka::Event::EVENT_ERROR:
        if (event.fatal()) {
          std::cerr << "FATAL ";
          run = false;
        }
        std::cerr << "ERROR (" << RdKafka::err2str(event.err()) << "): " <<
            event.str() << std::endl;
        break;

      case RdKafka::Event::EVENT_STATS:
        std::cerr << "\"STATS\": " << event.str() << std::endl;
        break;

      case RdKafka::Event::EVENT_LOG:
        fprintf(stderr, "LOG-%i-%s: %s\n",
                event.severity(), event.fac().c_str(), event.str().c_str());
        break;

      default:
        std::cerr << "EVENT " << event.type() <<
            " (" << RdKafka::err2str(event.err()) << "): " <<
            event.str() << std::endl;
        break;
    }
  }
};

void msg_consume(RdKafka::Message* message, void* opaque, char* buf) {
  const RdKafka::Headers *headers;

  switch (message->err()) {
    case RdKafka::ERR__TIMED_OUT:
      break;

    case RdKafka::ERR_NO_ERROR:
    {
      /* Real message */
      std::cout << "Read msg at offset " << message->offset() << std::endl;
      if (message->key()) {
        std::cout << "Key: " << *message->key() << std::endl;
      }
      headers = message->headers();
      if (headers) {
        std::vector<RdKafka::Headers::Header> hdrs = headers->get_all();
        for (size_t i = 0 ; i < hdrs.size() ; i++) {
          const RdKafka::Headers::Header hdr = hdrs[i];
          if(hdr.key().find("timestamp"))
          {
              //std::cout << "Timestamp present" << std::endl;
              //std::cout << "Timestamp: " << *(uint64_t*)hdr.value() << std::endl;
              //memcpy((void*)buf + sizeof(uint64_t)/*sizeofseqno*/, hdr.value(), sizeof(uint64_t));
          }
          if (hdr.value() != NULL)
            printf(" Header: %s = \"%.*s\"\n",
                   hdr.key().c_str(),
                   (int)hdr.value_size(), (const char *)hdr.value());
          else
            printf(" Header:  %s = NULL\n", hdr.key().c_str());
        }
      }
      //memcpy((void*)buf, message->payload(), message->len());
      printf("%.*s\n",
        static_cast<int>(message->len()),
        static_cast<const char *>(message->payload()));
      break;
    }
    case RdKafka::ERR__PARTITION_EOF:
      /* Last message */
      if (exit_eof) {
        run = false;
      }
      break;

    case RdKafka::ERR__UNKNOWN_TOPIC:
    case RdKafka::ERR__UNKNOWN_PARTITION:
      std::cerr << "Consume failed: " << message->errstr() << std::endl;
      run = false;
      break;

    default:
      /* Errors */
      std::cerr << "Consume failed: " << message->errstr() << std::endl;
      run = false;
  }
}

class ExampleConsumeCb : public RdKafka::ConsumeCb {
 public:
  void consume_cb (RdKafka::Message &msg, void *opaque) {
    msg_consume(&msg, opaque,buf);
  }
  char * buf;
};

void *runConsumerProducer(void *argument);

class ConsumerProducer
{
    public:
    ConsumerProducer(const std::string &send, const std::string &receive, bool isProducer) : brokers_("localhost:9092")
        , msg_len(128)
        , timeStamp(0)
        , topicSendStr_(send)
        , topicReceiveStr_(receive)
        , running_(false)
        , isProducer_(isProducer)
        , partition_(RdKafka::Topic::PARTITION_UA)
        , startOffset_(RdKafka::Topic::OFFSET_BEGINNING)
        , tconf_(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC))
    {
    }
    void initialise()
    {
        std::string errstr;
        /*
         * Create configuration objects
        */
        RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
        //set conf fields
        conf->set("metadata.broker.list", brokers_, errstr);
        conf->set("event_cb", &exEventCb_, errstr);
        conf->set("dr_cb", &exDrCb_, errstr);
        conf->set("default_topic_conf", tconf_, errstr);

        //create producer
        producer_ = RdKafka::Producer::create(conf, errstr);
        if (!producer_) {
          std::cerr << "Failed to create producer: " << errstr << std::endl;
          exit(1);
        }
        std::cout << "% Created producer " << producer_->name() << std::endl;

        //createConsumer
        conf->set("enable.partition.eof", "true", errstr);
        consumer_ = RdKafka::Consumer::create(conf, errstr);
        if (!consumer_) {
          std::cerr << "Failed to create consumer: " << errstr << std::endl;
          exit(1);
        }
        std::cout << "% Created consumer " << consumer_->name() << std::endl;

        //Publish in main thread
        if(isProducer_)
            pthread_create (&thread_, NULL, runConsumerProducer, (void*) this);


    }
    void stop()
    {
        running_ = false;
        //Only for producer
        if(isProducer_)
            pthread_join(thread_, NULL);
    }
    void publish()
    {
        std::cout << "Publishing..." << std::endl;

        RdKafka::Headers *headers = RdKafka::Headers::create();
        headers->add("my header", "header value");
        headers->add("other header", "yes");

        char* p = buf;
        memset (p, 'A', msg_len);
        memset (p, (uint64_t)seqno, sizeof (uint64_t));

        uint64_t seqno = (uint64_t)p;
        timeval currentTime;
        gettimeofday(&currentTime, NULL);
        if(timeStamp <= 0)
        {
            memcpy (p + sizeof (uint64_t), &currentTime.tv_usec, sizeof(uint64_t));
        }
        /*
        * Produce message
        */
        RdKafka::ErrorCode resp =
        producer_->produce(topicSendStr_, partition_,
                            RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
                            /* Value */
                            p, msg_len,
                            /* Key */
                            NULL, 0,
                            /* Timestamp (defaults to now) */
                            currentTime.tv_usec,
                            /* Message headers, if any */
                            headers,
                            /* Per-message opaque value passed to
                            * delivery report */
                            NULL);
          if (resp != RdKafka::ERR_NO_ERROR) {
            std::cerr << "% Produce failed: " <<
              RdKafka::err2str(resp) << std::endl;
            delete headers; /* Headers are automatically deleted on produce()
                             * success. */
          } else {
            //std::cerr << "% Produced message (" << line.size() << " bytes)" <<
             // std::endl;
          }

          producer_->poll(0);
    }
    void consume()
    {
        //std::cout << "Consuming..." << std::endl;
        partition_ = 0;
        int timeStamp;
        std::string errstr;
        /*
        * Create topic handle.
        */
        RdKafka::Topic *topic = RdKafka::Topic::create(consumer_, topicReceiveStr_,
						   tconf_, errstr);
        if (!topic) {
            std::cerr << "Failed to create topic: " << errstr << std::endl;
            exit(1);
        }

        /*
        * Start consumer for topic+partition at start offset
        */
        RdKafka::ErrorCode resp = consumer_->start(topic, partition_, startOffset_);
        if (resp != RdKafka::ERR_NO_ERROR) {
            std::cerr << "Failed to start consumer: " <<
	        RdKafka::err2str(resp) << std::endl;
            exit(1);
        }

        ExampleConsumeCb ex_consume_cb;

        /*
        * Consume messages
        */
        while (run) {
            RdKafka::Message *msg = consumer_->consume(topic, partition_, 1000);
            char* p = buf;
            msg_consume(msg, NULL, p);
            uint64_t ts = *(uint64_t*)(p + sizeof(uint64_t));
            timeval currentTime;
            gettimeofday(&currentTime, NULL);
            std::cout << "Latency: " << currentTime.tv_usec - ts << "microseconds"  << std::endl;
            delete msg;
            consumer_->poll(0);
        }

        /*
        * Stop consumer
        */
        consumer_->stop(topic, partition_);

        consumer_->poll(1000);
    }
    void consumeAndPublish()
    {
        //std::cout << "Consuming & Publishing..." << std::endl;
        partition_ = 0;
        int timeStamp;
        std::string value;
        std::string errstr;
        /*
        * Create topic handle.
        */
        RdKafka::Topic *topic = RdKafka::Topic::create(consumer_, topicReceiveStr_,
						   tconf_, errstr);
        if (!topic) {
            std::cerr << "Failed to create topic: " << errstr << std::endl;
            exit(1);
        }

        /*
        * Start consumer for topic+partition at start offset
        */
        RdKafka::ErrorCode resp = consumer_->start(topic, partition_, startOffset_);
        if (resp != RdKafka::ERR_NO_ERROR) {
            std::cerr << "Failed to start consumer: " <<
	        RdKafka::err2str(resp) << std::endl;
            exit(1);
        }

        ExampleConsumeCb ex_consume_cb;

        /*
        * Consume messages
        */
        while (run) {
            RdKafka::Message *msg = consumer_->consume(topic, partition_, 1000);
            char* p = buf;
            msg_consume(msg, NULL, p);
            delete msg;
            consumer_->poll(0);
            std::stringstream ss;
            ss << timeStamp;
            publish();
        }

        /*
        * Stop consumer
        */
        consumer_->stop(topic, partition_);

        consumer_->poll(1000);
    }
    //buffer properties
    char buf[1024];
    uint64_t seqno;
    uint64_t timeStamp;
    int msg_len;
    //Kafka properties
    RdKafka::Consumer *consumer_;
    RdKafka::Producer *producer_;
    ExampleDeliveryReportCb exDrCb_;
    ExampleEventCb exEventCb_;
    std::string brokers_;
    std::string topicSendStr_;
    std::string topicReceiveStr_;
    std::string messageValueStr_;
    bool running_;
    bool isProducer_;
    int32_t partition_;
    int64_t startOffset_;
    RdKafka::Conf *tconf_;
    private:
        pthread_t thread_;
};

void *runConsumerProducer(void *argument)
{
    ConsumerProducer* consumerProducer = (ConsumerProducer*) argument;
    //consumerProducer->running_ = true;
    //while(consumerProducer->running_)
    consumerProducer->consume();
    return 0;
}


void usage()
{
    std::cout << "TODO USAGE" << std::endl;
}


int main (int argc, char **argv) {
  std::string brokers = "localhost";
  std::string errstr;
  std::string topicSendStr;
  std::string topicReceiveStr;
  std::string mode;
  std::string debug;
  int opt;

  while ((opt = getopt(argc, argv, "PCs:r:b:")) != -1) {
    switch (opt) {
    case 'P':
    case 'C':
      mode = opt;
      break;
    case 'r':
      topicReceiveStr = optarg;
      break;
    case 's':
      topicSendStr = optarg;
      break;
    case 'b':
      brokers = optarg;
      break;
    default:
      usage();
      exit(1);
    }
  }

  signal(SIGINT, sigterm);
  signal(SIGTERM, sigterm);

  if(mode == "P")
  {
      std::string line = "HelloWorld";
      ConsumerProducer producer(topicSendStr, topicReceiveStr, true);
      producer.initialise();
      producer.publish();
      while(run)
          sleep(1);
      producer.stop();
  }
  if(mode == "C")
  {
      ConsumerProducer consumer(topicSendStr, topicReceiveStr, false);
      consumer.initialise();
      consumer.consumeAndPublish();
      while(run)
          sleep(1);
      consumer.stop();
  }
  return 0;
}
