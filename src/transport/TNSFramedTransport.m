/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#import "TNSFramedTransport.h"

@implementation TNSFramedTransport

- (id) initWithTransport: (id<TTransport>) transport {
    self = [super init];
    
    if (self) {
        _writeBuffer = [NSMutableData new];
        
        _readBuffer  = [NSMutableData new];
        _readBufferOffset = 0;
        
        _ttransport  = transport;
    }
    
    return self;
}


- (void) readFrame {
    _readBufferOffset = 0;

    // Read in the frame size
    uint8_t i32buf[4];
    [_ttransport readAll:(uint8_t*) &i32buf offset:0 length:4];
    
    // convert to an integer
    int frameSize = ((i32buf[0] & 0xff) << 24) |
                    ((i32buf[1] & 0xff) << 16) |
                    ((i32buf[2] & 0xff) <<  8) |
                    ((i32buf[3] & 0xff));

    [_readBuffer setLength:frameSize];
    
    // TODO: Should really check we got what we asked for, transport may throw an exception.
    [_ttransport readAll:(uint8_t *)[_readBuffer bytes] offset:0 length:frameSize];
}


- (int) readAll: (uint8_t *) buf offset: (int) off length: (int) len {
    // Could do a replaceBytesInRange to reduce the memory overhead of the 
    // read buffer, but given that most bufferes will be drained pretty quickly I'm chosing to 
    // skip this for moment.
	NSUInteger buflen = [_readBuffer length];
    if (buflen > 0 && buflen - _readBufferOffset > 0) {
        [_readBuffer getBytes:buf range:NSMakeRange(_readBufferOffset, len)];
        _readBufferOffset += len;
        if (_readBufferOffset == [_readBuffer length]) {
            [_readBuffer setLength:0];
        }
        return len;
    }
    
    // Nothing in the read buffer, so grab another frame
    [self readFrame];

    [_readBuffer getBytes:buf range:NSMakeRange(_readBufferOffset, len)];
    _readBufferOffset += len;

    return len;
}

- (void) write: (const uint8_t *) data offset: (unsigned int) offset length: (unsigned int) length {
    [_writeBuffer appendBytes:data+offset length: length];
}

- (void) flush {
    // Write out the frame size
    int frameSize = (int) [_writeBuffer length];
    
    uint8_t i32buf[4];
    i32buf[0] = (uint8_t)(0xff & (frameSize >> 24));
    i32buf[1] = (uint8_t)(0xff & (frameSize >> 16));
    i32buf[2] = (uint8_t)(0xff & (frameSize >> 8));
    i32buf[3] = (uint8_t)(0xff & (frameSize));

    [_ttransport write:(const uint8_t*) &i32buf     offset:0 length:4];
    
    // Send the data
    [_ttransport write:(const uint8_t*) [_writeBuffer bytes] offset:0 length:frameSize];
    [_ttransport flush];
    
    // Reset ready for next write
    _readBufferOffset = 0;
    [_writeBuffer setLength:0];
}

@end
