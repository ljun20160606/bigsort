# Bigsort

Bigsort is a example that sorts a big data in limited memory.

## 题目

    100GB url 文件，使用 1GB 内存计算出出现次数 top100 的 url 和出现的次数
    文件中 url 的分隔符按照 /n 换行
    文件中的 url 的长度范围可以认为是 1K 以内

## 思路

1. 将100GB文件根据hash(url)%1000分成1000个小于100M的文件，文件名1,2....，如果hash碰撞过多单个文件大小超过100M，记录文件为1-1，1-2继续拆分。
2. 以序列为一组如1，1-1，1-2...分别进行统计，一个url的散列表记录url出现的次数，然后在将一组的记录归并，因为一组内的url都是近似的，最多出现的url大小一定小于等于100M。
3. 每组记录的url最大为100kb，1gb / 100kb = 10485.76，最多允许拆分成10485.76这么多的文件，现在的分组最大为1000组，所以不虚，接着把所有分组的记录读取到全局的url记录表，进行整体的合并后，排序取出前100个url和记录。

## 如何使用本代码完成题目

```sh
# 先编译cmd
$ go build cmd/bigsort
# 默认 topNum=10 size=10
# topNum 记录前100的url; 生成的文件大小1024M * 10即10G
$ bigsort mock --topNum=100 --size=10240
生成checkpoint，以及mock数据mock-1.txt
# 默认 topNum=10 limitMemory=1 bucketSum=10
# topNum 计算前100的url; limitMemory 限制单文件大小100M; bucketSum hash范围为1000
$ bigsort compute --topNum=100 --limitMemory=100M --bucketSum=1000
结果和checkpoint种记录的正确答案
```

**如果只是为了完成题目的效果，按照默认的参数走就好了，不然mock数据生成的会有点慢，计算的速度也不太好，因为包含了大量的随机写**