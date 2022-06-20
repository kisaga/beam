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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.checkerframework.checker.nullness.qual.Nullable;

/** {@link Serializable} object to store twitter configurations for a connection. * */
@DefaultCoder(SerializableCoder.class)
public class TwitterConfig implements Serializable {
  private final String token;
  private final String language;
  private final Long tweetsCount;
  private final Integer minutesToRun;

  private TwitterConfig(TwitterConfig.Builder builder) {
    this.token = builder.token;
    this.language = builder.language;
    this.tweetsCount = builder.tweetsCount;
    this.minutesToRun = builder.minutesToRun;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TwitterConfig that = (TwitterConfig) o;
    return Objects.equals(token, that.token)
        && Objects.equals(language, that.language)
        && Objects.equals(tweetsCount, that.tweetsCount)
        && Objects.equals(minutesToRun, that.minutesToRun);
  }

  @Override
  public int hashCode() {
    return Objects.hash(token, language, tweetsCount, minutesToRun);
  }

  public String getToken() {
    return token;
  }

  public String getLanguage() {
    return language;
  }

  public Long getTweetsCount() {
    return tweetsCount;
  }

  public Integer getMinutesToRun() {
    return minutesToRun;
  }

  public static class Builder {
    private String token = "";
    private String language = "en";
    private Long tweetsCount = Long.MAX_VALUE;
    private Integer minutesToRun = Integer.MAX_VALUE;

    TwitterConfig.Builder setToken(final String token) {
      this.token = token;
      return this;
    }

    TwitterConfig.Builder setLanguage(final String language) {
      this.language = language;
      return this;
    }

    TwitterConfig.Builder setTweetsCount(final Long tweetsCount) {
      this.tweetsCount = tweetsCount;
      return this;
    }

    TwitterConfig.Builder setMinutesToRun(final Integer minutesToRun) {
      this.minutesToRun = minutesToRun;
      return this;
    }

    TwitterConfig build() {
      return new TwitterConfig(this);
    }
  }
}
