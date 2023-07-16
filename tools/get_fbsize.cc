#include <iostream>

#include <sys/statvfs.h>

int main() {
    struct statvfs *buf = new(struct statvfs);

    statvfs("../super_reader/test_file", buf);

    std::cout << buf->f_bsize << std::endl;
    std::cout << buf->f_frsize << std::endl;
    std::cout << buf->f_blocks << std::endl;

    delete buf;
    return 0;
}
