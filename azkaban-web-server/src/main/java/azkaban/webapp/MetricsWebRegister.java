package azkaban.webapp;


import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Gauge;

import azkaban.executor.ExecutorManager;

public class MetricsWebRegister{
  private ExecutorManager _executorManager;
  private String endpointName;

  public MetricsWebRegister(MetricsWebRegisterBuilder builder) {
    this.endpointName = builder.endpointName;
    this._executorManager = builder._executorManager;
  }

  public void addExecutorManagerMetrics(MetricRegistry metrics) throws Exception {
    if (_executorManager == null)
      throw new Exception("TODO: ");

    metrics.register("WEB-NumRunningFlows", new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return _executorManager.getRunningFlows().size();
      }
    });

    metrics.register("WEB-NumQueuedFlows", new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return _executorManager.getQueuedFlowNum();
      }
    });
  }

  public static class MetricsWebRegisterBuilder {
    private ExecutorManager _executorManager;
    private String endpointName;

    public MetricsWebRegisterBuilder(String endpointName) {
      this.endpointName = endpointName;
    }

    public MetricsWebRegisterBuilder addExecutorManager(ExecutorManager executorManager) {
      this._executorManager = executorManager;
      return this;
    }

    public MetricsWebRegister build() {
      return new MetricsWebRegister(this);
    }
  }

}
