<!--
 * @Description: README.md
 * @FilePath: /bull-golang/README.md
 * @Author: liyibing liyibing@lixiang.com
 * @Date: 2023-07-28 11:23:27
-->
# bull-golang

bull-js golang 版本

使用方法

样例见./example/example.go

导入包后使用定义结构体添加bull.QueueIface，该接口定义了Add方法
接受QueueOption将队列初始化，Option包括keyPrefix与QueueName与redis地址与登录密码

Add方法接受一个json格式的JobData与一系列提供的初始化方法
初始化方法是可选的，当前支持
WithPriorityOp(priority int)
WithRemoveOnCompleteOp(flag bool)
WithRemoveOnFailOp(flag bool)
WithAttemptsOp(times int)
WithDelayOp(delayTime int)
