# biotools.go
go database

## Usage

```
-i <--input csv file:原始数据文件
-g <--goda  csv file:GO子孙数据
-c <--check csv file:BP,MF,CC既要统计合并的GOID数据
-s <--settings csv file:配置文件
-t <--type :goid,goterm
-d <--desendant name prefix like BP,CC,MF
```
## Example
> 首次从远程数据库读取MF goterm并生成MF_descendant_ancestor.csv保存至本地
```
./app.py -i ./../data/20150430/MF/CDS-7-MF.csv -s settings.conf -c MF-GOTERM.txt -t goterm -d MF
```
> 从本地读取MF_descendant_ancestor.csv
```
./app.py -i ./../data/20150430/MF/CDS-H1-MF.csv -g ../result/MF_descendant_ancestor.csv -c MF-GOTERM.txt -t goterm
```


