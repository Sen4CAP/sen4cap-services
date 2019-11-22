/*
 *
 *  * Copyright (C) 2019 CS ROMANIA
 *  *
 *  * This program is free software; you can redistribute it and/or modify it
 *  * under the terms of the GNU General Public License as published by the Free
 *  * Software Foundation; either version 3 of the License, or (at your option)
 *  * any later version.
 *  * This program is distributed in the hope that it will be useful, but WITHOUT
 *  * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 *  * more details.
 *  *
 *  * You should have received a copy of the GNU General Public License along
 *  * with this program; if not, see http://www.gnu.org/licenses/
 *
 */

package org.esa.sen4cap.preprocessing;

final class ProcessFlag {
    static final int AMPLITUDE = 1;
    static final int COHERENCE = 2;
    static final int OVERWRITE = 4;

    static boolean isSet(int flags, int mask) {
        return (flags & mask) != 0;
    }

    static boolean isReset(int flags, int mask) {
        return (flags & mask) == 0;
    }

    static int resetBit(int flags, int mask) { return flags & ~mask; }

    static int setBit(int flags, int mask) { return flags | mask; }

}
