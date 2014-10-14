<h1>easydfs 分布式文件系统</h1>
<h2>为什么做easydfs</h2>

<p>
  &nbsp;&nbsp;与传统的分布式文件系统类似, 将上传的文件分散的每个实体机上, 达到分担压力的效果。
</p>
<h2>easydfs功能</h2>
<p>
	<ul>
		<li>文件上传下载</li>
		<li>server检测client可用性</li>
		<li>replica自动备份shared数据</li>
		<li>节点选举</li>
		<li>文件存储分片</li>
	</ul>
</p>
<h2>easydfs缺点</h2>
<p>中心化server缺点, 由于所有的数据、文件、指令都是从server发送到client, 所以server端压力过大, 目前主流的分布式文件存储都是去中心化思想。
<h2>easydfs 快速入门</h2>

<h3>
  EasyDFSServer
</h3>
<h4>主要功能</h4>
<p>EasyDFSServer中心, 主要功能为监控、调解client、文件上传、文件下载等。</p>
<h4>启动指令</h4>
<p>-shareds=1 -folder=D:\nio\server\</p>
<ul>
	<li>-shareds：分片数量</li>
	<li>-folder：server存储路径(只存储client文件索引、文件版本等)</li>
</ul>
<h3>
  EasyDFSClient
</h3>
<h4>主要功能</h4>
<p>EasyDFSClient客户端, 主要功能为文件存储</p>
<h4>启动指令</h4>
<p>-folder=D:\nio\client1\</p>
<ul>
	<li>-folder：client存储路径(存储文件、文件版本)</li>
</ul>

<h3>
  EasyDFSObserver
</h3>
<h4>主要功能</h4>
<p>节点监控、选举</p>

<h3>
  ReplicaCollect
</h3>
<h4>主要功能</h4>
<p>client版本收集</p>

<h3>
  RepliSync
</h3>
<h4>主要功能</h4>
<p>replica同步shared</p>


<h3>
  EasyDFSDownload
</h3>
<h4>主要功能</h4>
<p>文件下载API</p>


<h3>
  EasyDFSUpload
</h3>
<h4>主要功能</h4>
<p>文件上传API</p>
