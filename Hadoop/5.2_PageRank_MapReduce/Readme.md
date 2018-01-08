# PageRank - Thibaut DRAPP & Emilien DUFOUR #


Here are the PageRank results for different initializations.

![first_equation](http://latex.codecogs.com/gif.latex?%24%24%5Cfrac%7B%281-%5Calpha%29%7D%7BN%7D%20&plus;%20%5Calpha%20%5Csum_%7Bm%20%7D%7B%5Cfrac%7BP%28m%29%7D%7BL%28m%29%7D%7D%24%24)

### 1. ###
* P(n) Initialization : 1.0       

* Iterations : 35           

* Result 10 HIGHEST(ASC order):

PageRank | Node Id
------------ | -------------
0.013951332726410715  |  51886
0.013958969337990679  |  18130
0.016982516000856672  |  69130
0.017196379266760126  |  71308
0.020358162617382995  |  72114
0.020381593933649623  |  7832
0.02299738007399932   |  14574
0.03399661380574031   |  43990
0.04072312097633631   |  15048
0.04430665884306643   |  8648



### 2. ###
* P(n) Initialization : 1/N     

* Iterations : 100


* Result 10 HIGHEST (ASC order)

PageRank | Node Id
------------ | -------------
1.1468513967742779E-4  |  390
1.1591555095974218E-4  |  118
1.230271507321131E-4   |  136
1.4880240024613633E-4  |  1719
1.4978962594672552E-4  |  790
1.7389333028655448E-4  |  1753
1.7923668445263039E-4  |  737
1.8068301860223E-4     |  751
2.415873745429418E-4   | 4415
3.888618451102554E-4   | 18
