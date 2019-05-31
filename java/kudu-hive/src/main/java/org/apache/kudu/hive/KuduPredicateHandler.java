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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler.DecomposedPredicate;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduPredicate;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import java.util.ArrayList;
import java.util.List;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class KuduPredicateHandler {

  public static DecomposedPredicate decompose(ExprNodeDesc desc, Schema schema) {
    IndexPredicateAnalyzer analyzer = newAnalyzer(schema);
    List<IndexSearchCondition> sConditions = new ArrayList<>();
    ExprNodeDesc residualPredicate = analyzer.analyzePredicate(desc, sConditions);

    // Nothing to decompose.
    if (sConditions.size() == 0) {
      return null;
    }

    DecomposedPredicate decomposedPredicate = new DecomposedPredicate();
    decomposedPredicate.pushedPredicate = analyzer.translateSearchConditions(sConditions);
    decomposedPredicate.residualPredicate = (ExprNodeGenericFuncDesc) residualPredicate;
    return decomposedPredicate;
  }

  public static List<KuduPredicate> getPredicates(Configuration conf, Schema schema) {
    List<KuduPredicate> predicates = new ArrayList<>();
    for (IndexSearchCondition sc : getSearchConditions(conf, schema)) {
      predicates.add(conditionToPredicate(sc, schema));
    }
    return predicates;
  }

  private static List<IndexSearchCondition> getSearchConditions(Configuration conf, Schema schema) {
    List<IndexSearchCondition> conditions = new ArrayList<>();
    ExprNodeDesc filterExpr = getExpression(conf);
    if (null == filterExpr) {
      return conditions;
    }
    IndexPredicateAnalyzer analyzer = newAnalyzer(schema);
    ExprNodeDesc residual = analyzer.analyzePredicate(filterExpr, conditions);
    if (residual != null) {
      throw new RuntimeException("Unexpected residual predicate: " + residual.getExprString());
    }
    return conditions;
  }

  private static ExprNodeDesc getExpression(Configuration conf) {
    String filteredExprSerialized = conf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
    if (filteredExprSerialized == null) {
      return null;
    }
    return SerializationUtilities.deserializeExpression(filteredExprSerialized);
  }

  // TODO: Handle HiveDecimal conversion?
  private static KuduPredicate conditionToPredicate(IndexSearchCondition condition, Schema schema) {
    ColumnSchema column = schema.getColumn(condition.getColumnDesc().getColumn());
    GenericUDF genericUDF = condition.getOriginalExpr().getGenericUDF();
    Object value = condition.getConstantDesc().getValue();
    if (genericUDF instanceof GenericUDFOPEqual) {
      return KuduPredicate.newComparisonPredicate(column, KuduPredicate.ComparisonOp.EQUAL, value);
    } else if (genericUDF instanceof GenericUDFOPGreaterThan) {
      return KuduPredicate.newComparisonPredicate(column, KuduPredicate.ComparisonOp.GREATER, value);
    } else if (genericUDF instanceof GenericUDFOPEqualOrGreaterThan) {
      return KuduPredicate.newComparisonPredicate(column, KuduPredicate.ComparisonOp.GREATER_EQUAL, value);
    } else if (genericUDF instanceof GenericUDFOPLessThan) {
      return KuduPredicate.newComparisonPredicate(column, KuduPredicate.ComparisonOp.LESS, value);
    } else if (genericUDF instanceof GenericUDFOPEqualOrLessThan) {
      return KuduPredicate.newComparisonPredicate(column, KuduPredicate.ComparisonOp.LESS_EQUAL, value);
    } else if (genericUDF instanceof GenericUDFOPNull) {
      return KuduPredicate.newIsNullPredicate(column);
    } else if (genericUDF instanceof GenericUDFOPNotNull) {
      return KuduPredicate.newIsNotNullPredicate(column);
    } else {
      throw new RuntimeException("Unhandled Predicate: " + genericUDF);
    }
  }

  private static IndexPredicateAnalyzer newAnalyzer(Schema schema) {
    IndexPredicateAnalyzer analyzer = new IndexPredicateAnalyzer();

    // Register comparison operators which can be satisfied by Kudu predicates.
    analyzer.addComparisonOp(GenericUDFOPEqual.class.getName());
    analyzer.addComparisonOp(GenericUDFOPGreaterThan.class.getName());
    analyzer.addComparisonOp(GenericUDFOPEqualOrGreaterThan.class.getName());
    analyzer.addComparisonOp(GenericUDFOPLessThan.class.getName());
    analyzer.addComparisonOp(GenericUDFOPEqualOrLessThan.class.getName());
    analyzer.addComparisonOp(GenericUDFOPNull.class.getName());
    analyzer.addComparisonOp(GenericUDFOPNotNull.class.getName());
    // TODO: Handle IN and AND predicates.
    //analyzer.addComparisonOp(GenericUDFIn.class.getName());
    //analyzer.addComparisonOp(GenericUDFOPAnd.class.getName());

    // Set the column names that can be satisfied.
    for (ColumnSchema col : schema.getColumns()) {
      analyzer.allowColumnName(col.getName());
    }

    return analyzer;
  }
}
