// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.kudu.hive;

import java.security.AccessController;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.security.auth.Subject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.kudu.client.KuduClient;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kudu.hive.KuduStorageHandler.KUDU_TABLE_PROPERTIES;

/**
 * A collection of static utility methods for the Kudu Hive integration.
 * This is useful for code sharing.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class KuduHiveUtils {

  private static final Logger LOG = LoggerFactory.getLogger(KuduHiveUtils.class);

  private static final Text KUDU_TOKEN_KIND = new Text("kudu-authn-data");

  /**
   * Returns the union of the configuration and table properties with the
   * table properties taking precedence.
   */
  public static Configuration createOverlayedConf(Configuration conf, Properties tblProps) {
    Configuration newConf = new Configuration(conf);
    for (Map.Entry<Object, Object> prop : tblProps.entrySet()) {
      newConf.set((String) prop.getKey(), (String) prop.getValue());
    }
    return newConf;
  }

  // TODO: Support more configurations.
  //  Consider a Properties based setter on the KuduClientBuilder.
  public static KuduClient getKuduClient(Configuration conf) {
    String masterAddresses = conf.get(KuduStorageHandler.KUDU_MASTER_ADDRS_KEY);
    KuduClient client = new KuduClient.KuduClientBuilder(masterAddresses).build();
    importCredentialsFromCurrentSubject(client);
    return client;
  }

  public static void importCredentialsFromCurrentSubject(KuduClient client) {
    Subject subj = Subject.getSubject(AccessController.getContext());
    if (subj == null) {
      return;
    }
    Text service = new Text(client.getMasterAddressesAsString());
    // Find the Hadoop credentials stored within the JAAS subject.
    Set<Credentials> credSet = subj.getPrivateCredentials(Credentials.class);
    for (Credentials creds : credSet) {
      for (Token<?> tok : creds.getAllTokens()) {
        if (!tok.getKind().equals(KUDU_TOKEN_KIND)) {
          continue;
        }
        // Only import credentials relevant to the service corresponding to
        // 'client'. This is necessary if we want to support a job which
        // reads from one cluster and writes to another.
        if (!tok.getService().equals(service)) {
          LOG.debug("Not importing credentials for service " + service +
              "(expecting service " + service + ")");
          continue;
        }
        LOG.debug("Importing credentials for service " + service);
        client.importAuthenticationCredentials(tok.getPassword());
        return;
      }
    }
  }
}
