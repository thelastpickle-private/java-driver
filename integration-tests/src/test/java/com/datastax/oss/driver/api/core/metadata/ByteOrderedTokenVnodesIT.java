/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.api.core.metadata;

import com.datastax.oss.driver.api.core.Cluster;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.cluster.ClusterRule;
import com.datastax.oss.driver.categories.IsolatedTests;
import com.datastax.oss.driver.internal.core.metadata.token.ByteOrderedToken;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

@Category(IsolatedTests.class)
public class ByteOrderedTokenVnodesIT extends TokenITBase {

  @ClassRule
  public static CustomCcmRule ccmRule =
      CustomCcmRule.builder()
          .withNodes(3)
          .withCreateOption("-p ByteOrderedPartitioner")
          .withCreateOption("--vnodes")
          .build();

  @ClassRule
  public static ClusterRule clusterRule =
      new ClusterRule(ccmRule, false, true, "request.timeout = 30 seconds");

  public ByteOrderedTokenVnodesIT() {
    super(ByteOrderedToken.class, true);
  }

  @Override
  protected Cluster cluster() {
    return clusterRule.cluster();
  }

  @Override
  protected Session session() {
    return clusterRule.session();
  }

  @BeforeClass
  public static void createSchema() {
    TokenITBase.createSchema(clusterRule.session());
  }
}