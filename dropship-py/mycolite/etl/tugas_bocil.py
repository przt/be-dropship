def jadwalMakan(jam):
    if jam == 7 or jam == 8:
        print('Jam Makan Pagi')
    elif jam == 11 or jam == 12 or jam == 13:
        print('Jam Makan Siang')
    elif jam == 17 or jam == 18 or jam == 19:
        print('Jam Makan Malam')
    else:
        print('Belum jam makan')
jam = input("Masukkan Jam : ")
jadwalMakan(int(jam))