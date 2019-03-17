from main import *

def test_add_stats():
    S1 = Stats(1, 2, 3, 4)
    S3 = Stats(1, 2, 3, 4)
    S2 = Stats(2, 3, 4, 5)
    print(S1 + S2)

def test_build_stats():
    B = Build(0, 0, None)
    B.partitions = [Partition(1, 0, 0, Stats(1, 2, 3, 4)),
                    Partition(2, 0, 0, Stats(2, 3, 4, 5))]
    print(B.stats())

def test_build():
    build_size = math.ceil(1.5 * 256)
    probe_size = 0
    mem = 256
    F = 1.3
    partitions = 2

    J = Join(build_size, probe_size, mem, F, partitions)
    J.build.run()
    print(J.stats())
    print("J.spilledStatus =", J.spilledStatus)

def test_build_probe():
    build_size = math.ceil(1.5 * 256)
    probe_size = 0
    mem = 256
    F = 1.3
    partitions = 2

    J = Join(build_size, probe_size, mem, F, partitions)
    J.run()
    print(J.stats())
    print("J.spilledStatus =", J.spilledStatus)


if __name__ == '__main__':
    print('### Testing simple stat print')
    s = Stats()
    print(s)
    print()

    print('### Testing test_add_stats')
    test_add_stats()
    print()


    print('### Testing test_build_stats')
    test_build_stats()
    print()

    print('### Testing test_build')
    test_build()
    print()

    print('### Testing test_build_probe')
    test_build_probe()
    print()




