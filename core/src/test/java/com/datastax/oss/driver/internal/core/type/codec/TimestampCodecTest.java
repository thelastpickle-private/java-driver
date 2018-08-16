/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.type.codec;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.junit.Test;

public class TimestampCodecTest extends CodecTestBase<Instant> {

  public TimestampCodecTest() {
    codec = TypeCodecs.TIMESTAMP;
  }

  @Test
  public void should_encode() {
    assertThat(encode(Instant.EPOCH)).isEqualTo("0x0000000000000000");
    assertThat(encode(Instant.ofEpochMilli(128))).isEqualTo("0x0000000000000080");
    assertThat(encode(null)).isNull();
  }

  @Test
  public void should_decode() {
    assertThat(decode("0x0000000000000000").toEpochMilli()).isEqualTo(0);
    assertThat(decode("0x0000000000000080").toEpochMilli()).isEqualTo(128);
    assertThat(decode(null)).isNull();
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_decode_if_not_enough_bytes() {
    decode("0x0000");
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_decode_if_too_many_bytes() {
    decode("0x0000000000000000" + "0000");
  }

  @Test
  public void should_format() {
    // No need to test various values because the codec delegates directly to SimpleDateFormat,
    // which we assume does its job correctly.
    assertThat(format(Instant.EPOCH)).isEqualTo("'1970-01-01T00:00:00.000Z'");
    assertThat(format(Instant.parse("2018-08-16T15:59:34.123Z")))
        .isEqualTo("'2018-08-16T15:59:34.123Z'");
    assertThat(format(null)).isEqualTo("NULL");
  }

  @Test
  public void should_parse() {
    // Raw number
    assertThat(parse("'0'")).isEqualTo(Instant.EPOCH);
    assertThat(parse("'-1'")).isEqualTo(Instant.EPOCH.minusMillis(1));

    // Date formats

    Instant expected;

    // date without time, without time zone
    expected = LocalDateTime.parse("2018-08-16T00:00").atZone(ZoneId.systemDefault()).toInstant();
    assertThat(parse("'2018-08-16'")).isEqualTo(expected);

    // date without time, with time zone
    expected = LocalDateTime.parse("2018-08-16T00:00").atZone(ZoneOffset.ofHours(2)).toInstant();
    assertThat(parse("'2018-08-16 CEST'")).isEqualTo(expected);
    assertThat(parse("'2018-08-16+02'")).isEqualTo(expected);
    assertThat(parse("'2018-08-16+0200'")).isEqualTo(expected);
    assertThat(parse("'2018-08-16+02:00'")).isEqualTo(expected);

    // date with time, without time zone
    expected = LocalDateTime.parse("2018-08-16T16:08").atZone(ZoneId.systemDefault()).toInstant();
    assertThat(parse("'2018-08-16T16:08'")).isEqualTo(expected);
    assertThat(parse("'2018-08-16 16:08'")).isEqualTo(expected);

    // date with time + seconds, without time zone
    expected =
        LocalDateTime.parse("2018-08-16T16:08:38").atZone(ZoneId.systemDefault()).toInstant();
    assertThat(parse("'2018-08-16T16:08:38'")).isEqualTo(expected);
    assertThat(parse("'2018-08-16 16:08:38'")).isEqualTo(expected);

    // date with time + seconds + milliseconds, without time zone
    expected =
        LocalDateTime.parse("2018-08-16T16:08:38.230").atZone(ZoneId.systemDefault()).toInstant();
    assertThat(parse("'2018-08-16T16:08:38.230'")).isEqualTo(expected);
    assertThat(parse("'2018-08-16 16:08:38.230'")).isEqualTo(expected);

    // date with time, with time zone
    expected = ZonedDateTime.parse("2018-08-16T16:08:00.000+02:00").toInstant();
    assertThat(parse("'2018-08-16T16:08 CEST'")).isEqualTo(expected);
    assertThat(parse("'2018-08-16T16:08+02'")).isEqualTo(expected);
    assertThat(parse("'2018-08-16T16:08+0200'")).isEqualTo(expected);
    assertThat(parse("'2018-08-16T16:08+02:00'")).isEqualTo(expected);
    assertThat(parse("'2018-08-16 16:08 CEST'")).isEqualTo(expected);
    assertThat(parse("'2018-08-16 16:08+02'")).isEqualTo(expected);
    assertThat(parse("'2018-08-16 16:08+0200'")).isEqualTo(expected);
    assertThat(parse("'2018-08-16 16:08+02:00'")).isEqualTo(expected);

    // date with time + seconds, with time zone
    expected = ZonedDateTime.parse("2018-08-16T16:08:38.000+02:00").toInstant();
    assertThat(parse("'2018-08-16T16:08:38 CEST'")).isEqualTo(expected);
    assertThat(parse("'2018-08-16T16:08:38+02'")).isEqualTo(expected);
    assertThat(parse("'2018-08-16T16:08:38+0200'")).isEqualTo(expected);
    assertThat(parse("'2018-08-16T16:08:38+02:00'")).isEqualTo(expected);
    assertThat(parse("'2018-08-16 16:08:38 CEST'")).isEqualTo(expected);
    assertThat(parse("'2018-08-16 16:08:38+02'")).isEqualTo(expected);
    assertThat(parse("'2018-08-16 16:08:38+0200'")).isEqualTo(expected);
    assertThat(parse("'2018-08-16 16:08:38+02:00'")).isEqualTo(expected);

    // date with time + seconds + milliseconds, with time zone
    expected = ZonedDateTime.parse("2018-08-16T16:08:38.230+02:00").toInstant();
    assertThat(parse("'2018-08-16T16:08:38.230 CEST'")).isEqualTo(expected);
    assertThat(parse("'2018-08-16T16:08:38.230+02'")).isEqualTo(expected);
    assertThat(parse("'2018-08-16T16:08:38.230+0200'")).isEqualTo(expected);
    assertThat(parse("'2018-08-16T16:08:38.230+02:00'")).isEqualTo(expected);
    assertThat(parse("'2018-08-16 16:08:38.230 CEST'")).isEqualTo(expected);
    assertThat(parse("'2018-08-16 16:08:38.230+02'")).isEqualTo(expected);
    assertThat(parse("'2018-08-16 16:08:38.230+0200'")).isEqualTo(expected);
    assertThat(parse("'2018-08-16 16:08:38.230+02:00'")).isEqualTo(expected);

    assertThat(parse("NULL")).isNull();
    assertThat(parse("null")).isNull();
    assertThat(parse("")).isNull();
    assertThat(parse(null)).isNull();
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_parse_invalid_input() {
    parse("not a timestamp");
  }
}
