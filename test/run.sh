rm /tmp/clftest -r
rm nohup.out
g++ -std=c++11 -o test -g -I.. test.cc -L../lib -lengine -lpthread -lrt -lz
./test
