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
package com.datastax.oss.driver.api.core.data;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

public interface SchemaAwareByIndex extends AccessibleByIndex {

  /**
   * Returns the CQL type of the {@code i}th value.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  DataType getType(int i);

  @Override
  default <T> TypeCodec<T> codecFor(int i, GenericType<T> targetType) {
    return codecRegistry().codecFor(getType(i), targetType);
  }

  @Override
  default <T> TypeCodec<T> codecFor(int i, Class<T> targetType) {
    return codecRegistry().codecFor(getType(i), targetType);
  }

}
