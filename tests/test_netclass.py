import unittest
import netclass as nc

class NetclassTestCase(unittest.TestCase):
    def test_parse_bw_stats(self):
        s = """
class htb 1:10 root prio 0 rate 664Mbit ceil 664Mbit burst 1494b cburst 1494b
 Sent 0 bytes 0 pkt (dropped 0, overlimits 0 requeues 0)
 rate 0bit 0pps backlog 0b 0p requeues 0
 lended: 0 borrowed: 0 giants: 0
 tokens: 281 ctokens: 281

class htb 1:1 root prio 0 rate 10Gbit ceil 10Gbit burst 0b cburst 0b
 Sent 233962506 bytes 628235 pkt (dropped 0, overlimits 0 requeues 0)
 rate 281672bit 89pps backlog 0b 0p requeues 0
 lended: 581105 borrowed: 0 giants: 0
 tokens: 13 ctokens: 13
"""
        self.assertEqual(nc.NetClass.parseBwStats(s), {10: 0.0, 1: 281672 / 1000000.0})

        s = """
class htb 1:10 root prio 0 rate 662Mbit ceil 662Mbit burst 1489b cburst 1489b
         Sent 0 bytes 0 pkt (dropped 0, overlimits 0 requeues 0)
         rate 0bit 0pps backlog 0b 0p requeues 0
         lended: 0 borrowed: 0 giants: 0
         tokens: 281 ctokens: 281

        class htb 1:1 root prio 0 rate 10Gbit ceil 10Gbit burst 0b cburst 0b
         Sent 2445535363 bytes 10892289 pkt (dropped 0, overlimits 0 requeues 0)
         rate 2395Kbit 1323pps backlog 0b 0p requeues 0
         lended: 10889769 borrowed: 0 giants: 0
         tokens: 13 ctokens: 13
"""
        self.assertEqual(nc.NetClass.parseBwStats(s), {10: 0.0, 1: 2395000 / 1000000.0})

        s = """
class htb 1:10 root prio 0 rate 662Mbit ceil 662Mbit burst 1489b cburst 1489b
         Sent 0 bytes 0 pkt (dropped 0, overlimits 0 requeues 0)
         rate 123Mbit 0pps backlog 0b 0p requeues 0
         lended: 0 borrowed: 0 giants: 0
         tokens: 281 ctokens: 281

        class htb 1:1 root prio 0 rate 10Gbit ceil 10Gbit burst 0b cburst 0b
         Sent 2445535363 bytes 10892289 pkt (dropped 0, overlimits 0 requeues 0)
         rate 2395Kbit 1323pps backlog 0b 0p requeues 0
         lended: 10889769 borrowed: 0 giants: 0
         tokens: 13 ctokens: 13
"""
        self.assertEqual(nc.NetClass.parseBwStats(s), {10: 123000000 / 1000000.0, 1: 2395000 / 1000000.0})

if __name__ == '__main__':
    unittest.main()
