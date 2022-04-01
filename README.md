# person-century-cache
cache system for century associated with persons

```
python3 createcache.py <pathtopersonsgitrepo>
curl -X PUT -H Content-Type:text/turtle -T centuries.ttl -G http://buda1.bdrc.io:13180/fuseki/corerw/data --data-urlencode 'graph=http://purl.bdrc.io/graph/centuries'
```