# be-controller
This is the python implementation of the best effort controller. 

**Files description:**

* **maincontrol.py**: main controller code, also manages CPU shares
* **settings.py**: utility classes and global variables
* **netclass.py**: network utilities class
* **netcontrol**: network controller
* __init__.py: necessary for python import commands
* **config.json**: configuration parameters
* **Dockerfile.controller**: builds a docker image for the controller
* **controller.daemonset.yaml**: a deployment of the controller as a daemon set on all nodes

**Usage:**
```usage: maincontrol.py [-h] [-v] [-c CONFIG]

optional arguments:
  -h, --help            show this help message and exit
  -v, --verbose         increase output verbosity
  -c CONFIG, --config CONFIG
                        configuration file (JSON)
```
If a configuration file is not given, it looks for `config.json` in the local directory. 

**Configuration parameters:**

* "mode" : selects operating mode ("k8s" for kubernetes)
* "ctlloc" : does the controller run inside a container or not ("out"/"in")
* "default_class": the default class for pods not labeled with `hyperpilot.io/wclass:XX` ("HP")
* "period": the main controller period (5)
* "slack_threshold_disable": the SLO slack below which we disable BE pods (-0.5)
* "slack_threshold_disable": the SLO slack below which we disable BE pods (-0.2)
* "slack_threshold_shrink": the SLO slack below which we shrink BE pods (0.05)
* "slack_threshold_grow": the SLO slack above which we enable or grow BE pods (0.2)
* "load_threshold_shrink": the CPU load threshold after which we shrink BE pods (75.0)
* "load_threshold_grow": the CPU load threshold to which we allow BE pods to grow (60.0)
* "min_shares": the mimimum shares for a best effort controller, imposed by docker (2)
* "BE_growth_rate": rate of increasing shares for BE pods (1.1)
* "BE_shrink_rate": rate of decreasing shares for BE pods (0.8)
* "net_period": the network controller period (2)
* "iface_ext": the host interface on K8S nodes ("ens3")
* "iface_cont": the K8S interface on K8S nodes ("weave")
* "link_bw_mbps" : the maximum link bandwidth (10000)
* "max_bw_mbps" : the actual maximium bandwidth on this cluster (700)

**Labels**

The controller expects best effort pods to be marked with label `hyperpilot.io/wclass:BE`. All other workload either be marked as `hyperpilot.io/wclass:HP` or not marked at all. 

The controller expects to find exactly one pod in the whole cluster marked with `hyperpilot.io/qos: "true"`. This is the HP workload that the controller tries to read the SLO for. 

**BE On/Off**

The BE controller uses uses the `hyperpilot.io/be-enabled` node label to indicate if the node is currently accepting BE workloads or not. The value of the label is determined locally by the controller based on the SLO slack. It is best to issue BE workloads so that they are only scheduled to nodes with BE enabled. Use the following in deployment files: 

```annotations:
        scheduler.alpha.kubernetes.io/affinity: >
          {
            "nodeAffinity": {
              "requiredDuringSchedulingIgnoredDuringExecution": {
                "nodeSelectorTerms": [
                  {
                    "matchExpressions": [
                      {
                        "key": "hyperpilot.io/be-enabled",
                        "operator": "In",
                        "values": ["true"]
                      }
                    ]
                  }
                ]
              }
             }
          }
``` 

**Assumptions and Limitations**

The controller assumes a K8S cluster. It can run within a pod (ctlloc:"in") or on the node directy (ctlloc:"out"). When it runs within a pod, it can find the right credentials for K8S on its own. When it turns outside of a pod, it assume the credentials are at `~/.kube/config`. 

The controller assumes that the SLO for the HP pod monitored can be accessed from `qos-data-store:7781/v1/apps/metrics`. This is an issue when the controller is not running in a pod, as K8S DNS does not help with name resolution. 



