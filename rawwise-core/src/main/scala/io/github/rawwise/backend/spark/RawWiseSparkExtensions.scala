package io.github.rawwise.backend.spark

import org.apache.spark.sql.SparkSessionExtensions

class RawWiseSparkExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPostHocResolutionRule { session =>
      new LazyUpdateRewriteRule(session)
    }
    extensions.injectPostHocResolutionRule { session =>
      new RangeRoutingRule(session)
    }
    extensions.injectQueryPostPlannerStrategyRule { session =>
      new RawWisePreferredLocationRule(session)
    }
    extensions.injectParser { (session, parser) =>
      new RawWiseSqlParser(session, parser)
    }
    extensions.injectPlannerStrategy { session =>
      new PointUpdateStrategy()
    }
  }
}
