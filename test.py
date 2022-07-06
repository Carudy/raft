from rpcer import *


def cb1(bytes):
    return (bytes.decode() + ' ok.').encode()


def cb2(msg):
    for k, v in msg.items():
        print(f'{k}: {v}')
    msg['df'] *= 2
    return msg


if __name__ == '__main__':
    s = Grpcer(fmt='yaml')
    s.start_server(callback=cb2)

    c = Grpcer(fmt='yaml')
    m = {
        'a': 'fff0',
        'df': 434.54,
    }

    r = c.send(s, m)
    print(type(r), r)
