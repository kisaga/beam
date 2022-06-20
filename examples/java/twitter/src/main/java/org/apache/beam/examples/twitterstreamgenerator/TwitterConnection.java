/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.examples.twitterstreamgenerator;

import com.twitter.clientlib.TwitterCredentialsBearer;
import com.twitter.clientlib.model.StreamingTweetResponse;
import com.twitter.clientlib.query.StreamQueryParameters;
import com.twitter.clientlib.query.model.TweetField;
import com.twitter.clientlib.stream.TweetsStreamListener;
import com.twitter.clientlib.stream.TwitterStream;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/** Singleton class for twitter connection. * */
class TwitterConnection {
  private final BlockingQueue<StreamingTweetResponse> queue;
  private final TwitterStream twitterStream;
  private static final Object lock = new Object();
  static final ConcurrentHashMap<TwitterConfig, TwitterConnection> INSTANCE_MAP =
      new ConcurrentHashMap<>();

  /**
   * Creates a new Twitter connection.
   *
   * @param twitterConfig configuration for twitter connection
   */
  TwitterConnection(TwitterConfig twitterConfig) {
    this.queue = new LinkedBlockingQueue<>();

    this.twitterStream = new TwitterStream(new TwitterCredentialsBearer(twitterConfig.getToken()));
    TweetsStreamListener listener = new TweetsStreamListener() {
        @Override
        public void onTweetArrival(StreamingTweetResponse streamingTweet) {
            try {
                queue.offer(streamingTweet);
            } catch (Exception ignored) {
            }
        }
    };
    this.twitterStream.addListener(listener);
    StreamQueryParameters streamQueryParameters = new StreamQueryParameters.Builder()
            .withTweetFields(TweetField.CREATED_AT)
            .build();
    this.twitterStream.startSampleStream(streamQueryParameters);

  }

  public static TwitterConnection getInstance(TwitterConfig twitterConfig) {
    synchronized (lock) {
      if (INSTANCE_MAP.containsKey(twitterConfig)) {
        return INSTANCE_MAP.get(twitterConfig);
      }
      TwitterConnection singleInstance = new TwitterConnection(twitterConfig);
      INSTANCE_MAP.put(twitterConfig, singleInstance);
      return singleInstance;
    }
  }

  public BlockingQueue<StreamingTweetResponse> getQueue() {
    return this.queue;
  }

  public void closeStream() {
    this.twitterStream.shutdown();
  }
}
