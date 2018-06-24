# -*- coding: utf-8 -*-

# Copyright (c) 2016 RIPE NCC
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
from math import ceil
from tzlocal import get_localzone
from .base import Renderer as BaseRenderer

from ..helpers.colours import colourise
from ..helpers.sanitisers import sanitise
from ..ipdetails import IP


class Renderer(BaseRenderer):

    RENDERS = [BaseRenderer.TYPE_TRACEROUTE]
    TIME_FORMAT = "%a %b %d %H:%M:%S %Z %Y"
    DEFAULT_HIDE_ASNS = True
    DEFAULT_HIDE_ASN_ABNORMALITIES = True
    TIMING_BAR_WIDTH = 20

    @staticmethod
    def add_arguments(parser):
        group = parser.add_argument_group(
            title="Optional arguments for traceroute renderer"
        )
        group.add_argument(
            "--traceroute-hide-asns",
            help="Hide Autonomous System Numbers (ASNs) in the traceroute "
                 "results.",
            action="store_true",
            default=Renderer.DEFAULT_HIDE_ASNS
        )
        group.add_argument(
            "--traceroute-hide-asn-abnormalities",
            help="Hide abnormatlities when moving between ASNs in the "
                 "traceroute results.",
            action="store_true",
            default=Renderer.DEFAULT_HIDE_ASN_ABNORMALITIES
        )

    def __init__(self, *args, **kwargs):
        BaseRenderer.__init__(self, *args, **kwargs)
        self.f = open('myfile', 'w')
        self.f.write ('digraph G {\nfontsize=7\nfontcolor="#777777"\nlabeljust=r\nbgcolor="white"\nlabel="ripe-atlas"\n\n')
        self.nodes = set()
        self.edges = set()

    def _get_min_hop(self, packets):
        m = -1
        for p in packets:
            if p.rtt is None:
                continue
            if p.rtt < m or m is -1:
                m = p.rtt
        return m

    def _colourise_result(self, last, curr, next_):
        try:
            last_asn = IP(last.packets[0].origin).asn
        except TypeError:
            last_asn = ''

        try:
            curr_asn = IP(curr.packets[0].origin).asn
        except TypeError:
            curr_asn = ''

        try:
            next_asn = IP(next_.packets[0].origin).asn
        except TypeError:
            next_asn = ''
        except AttributeError:
            next_asn = ''

        last_min = self._get_min_hop(last.packets)
        curr_min = self._get_min_hop(curr.packets)
        try:
            next_min = self._get_min_hop(next_.packets)
        except AttributeError:
            next_min = -1

        if next_min is -1 or last_min is -1 or curr_min is -1:
            return "white"

        if curr_asn != last_asn:
            if curr_min > last_min * 1.5:
                return "red"
            if last_min > curr_min * 1.5:
                return "green"
        if curr_asn != next_asn:
            if curr_min > next_min * 1.5:
                return "red"
            if next_min > curr_min * 1.5:
                return "green"
        return "white"

    def _find_max_latency(self, results):
        max_ = -1

        for r in results.hops:
            avg = self._get_avg_latency(r)
            if avg > max_:
                max_ = avg

        return max_

    def _get_avg_latency(self, hop):

        def avg(ttls):
            ttls_ = [e for e in ttls if e != None]
            if ttls_ == []:
                return -1
            return (sum(ttls_) / len(ttls_))

        return avg([p.rtt for p in hop.packets])

    def _format_timing_bar(self, avg, max_avg):
        width = self.TIMING_BAR_WIDTH / max_avg * int(avg)
        if width < 0:
            width = 0

        return "|" * int(width)

    def on_result(self, result):

        r = ""

        i = -1

        max_latency = self._find_max_latency(result)

        for hop in result.hops:
            i = i + 1

            if hop.is_error:
                r += "{}\n".format(
                    colourise(sanitise(hop.error_message), "red"))
                continue

            name = ""
            asn = ""
            rtts = []
            for packet in hop.packets:
                name = name or packet.origin or "*"
                if packet.origin and not asn:
                    asn = IP(packet.origin).asn
                if packet.rtt:
                    rtts.append("{:8} ms".format(packet.rtt))
                else:
                    rtts.append("          *")

            min_rtt = self._get_min_hop(hop.packets)
            try:
                next_ = result.hops[i+1]
            except IndexError:
                next_ = None
            colour = self._colourise_result(
                result.hops[i-1], hop, next_
            )

            self._format_timing_bar(self._get_avg_latency(hop), max_latency)

            if not asn:
                tpl = "{hop:>3} {name:37} {rtts} {bars:8}\n"
            else:
                tpl = "{hop:>3} {name:28} {asn:>8} {rtts} {bars:8}\n"

            r += colourise(
                tpl.format(
                    hop=hop.index,
                    name=sanitise(name),
                    asn="AS{}".format(asn) if asn else "",
                    rtts="  ".join(rtts),
                    bars=self._format_timing_bar(
                        self._get_avg_latency(hop), max_latency)

                ),
                colour
            )
            if name != '*' and name not in self.nodes:
                self.f.write ('node{} [shape=rectangle color="black" fontsize=8 label="{}\\nAS{}" style=filled fillcolor="#ffffff" ]\n'.format(name.replace('.','_'),name,asn))
                self.nodes.add(name)

            if i != 0 and name+namebefore not in self.edges and name !='*' and namebefore != '*':
                self.f.write ('node{} -> node{} [color="#888887"] [penwidth=1]\n'.format(name.replace('.','_'),namebefore.replace('.','_')))
                self.edges.add(name+namebefore)
            namebefore = name

        created = result.created.astimezone(get_localzone())

        return "\n{}\n{}\n\n{}".format(
            colourise("Probe #{}".format(result.probe_id), "bold"),
            colourise(created.strftime(self.TIME_FORMAT), "bold"),
            r
        )
    def additional(self, results):
        self.f.write( '\n}\n')
        self.f.close()
        return
