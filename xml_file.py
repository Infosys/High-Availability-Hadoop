# The MIT License (MIT)

# Copyright (c) 2015 Infosys Ltd

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import xml.etree.ElementTree as ET

#wrapper on python xml library to provide higher level functions

class XML_File:
    def __init__ (self, ip_file):
        self.tree = ET.parse(ip_file)
        self.root = self.tree.getroot()

    def _locate_property (self, ip_property):
        assert(self.property_exists(ip_property))
        for property in self.root:
            for elem in property:
                if elem.text == ip_property and elem.tag == "name":
                    return property

    def property_exists(self, ip_property):
        for property in self.root:
            for elem in property:
                if elem.text == ip_property and elem.tag == "name":
                    return True
        
        return False

    def get_property(self, ip_property):
        if not self.property_exists(ip_property):
            return None

        property = self._locate_property(ip_property)

        for elem in property:
            if elem.tag == "value":
                return elem.text
        
    def _update_property(self, ip_property, ip_val):
        assert (self.property_exists(ip_property))
        property = self._locate_property(ip_property)

        for elem in property:
            if elem.tag == "value":
                elem.text = ip_val
                return


    def _add_property(self, ip_property, ip_val):
        assert( not self.property_exists (ip_property))
        new_property = ET.SubElement(self.root, "property")
        new_property.text = "\n"
        new_property.tail = "\n"

        name = ET.SubElement(new_property, "name")
        name.text = ip_property
        name.tail = "\n"

        val = ET.SubElement(new_property, "value")
        val.text = ip_val
        val.tail = "\n"

    def set_property(self, ip_property, ip_val):
        if self.property_exists(ip_property):
            self._update_property(ip_property, ip_val)
        else:
            self._add_property(ip_property, ip_val)

    def write (self, op_file):
        self.tree.write(op_file)

    def bulk_update(self, ip_props, tgt):
        for key in ip_props.keys():
            self.set_property(key, ip_props[key])

        self.write(tgt)


