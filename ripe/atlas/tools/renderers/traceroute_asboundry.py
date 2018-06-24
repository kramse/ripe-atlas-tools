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

from six import StringIO

from .base import Renderer as BaseRenderer
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
        super(Renderer, self).__init__(*args, **kwargs)

        self.nodes = set()
        self.edges = set()

        self.fh = StringIO()
        self.fh.write('digraph G {\nfontsize=7\nfontcolor="#777777"\nlabeljust=r\nbgcolor="white"\nlabel="ripe-atlas"\n\n')

    def on_result(self, result):
        i = -1

        for hop in result.hops:
            i += 1

            if hop.is_error:
                continue

            name = ""
            asn = ""
            for packet in hop.packets:
                name = name or packet.origin or "*"
                if packet.origin and not asn:
                    asn = IP(packet.origin).asn

            if name != '*' and name not in self.nodes:
                self.fh.write('node{} [shape=rectangle color="black" fontsize=8 label="{}\\nAS{}" style=filled fillcolor="#ffffff" ]\n'.format(name.replace('.', '_'), name, asn))
                self.nodes.add(name)

            if i != 0 and name + namebefore not in self.edges and name !='*' and namebefore != '*':
                self.fh.write('node{} -> node{} [color="#888887"] [penwidth=1]\n'.format(name.replace('.', '_'), namebefore.replace('.', '_')))
                self.edges.add(name + namebefore)
            namebefore = name
        return ""

    def additional(self, results):
        self.fh.write('\n}\n')
        try:
            return self.fh.getvalue()
        finally:
            self.fh.close()
