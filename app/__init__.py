"""条码标签 TCP 转发网关演示项目。

模块:
- config:   环境变量配置
- protocol: 终端/打印机之间的线路协议与标记
- gateway:  asyncio 双向转发网关(半关闭透传 + 写阻塞回收)
- printer:  模拟打印机(EOF 后回执 / 提前拒收 / 不读数据)
- terminal: 模拟条码终端的上传客户端(并发读写,可观测写阻塞)
- api:      FastAPI + Swagger 演示入口
"""

__version__ = "1.0.0"
