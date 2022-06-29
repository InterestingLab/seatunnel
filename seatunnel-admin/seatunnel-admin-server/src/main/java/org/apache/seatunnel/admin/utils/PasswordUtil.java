/*
 * Copyright 2002-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.admin.utils;

import cn.hutool.crypto.SecureUtil;

public class PasswordUtil {

    private PasswordUtil() {
        throw new IllegalStateException("PasswordUtil class");
    }

    public static String encrypt(String password, String salt) {
        String rawStr = password.concat(salt);
        return SecureUtil.md5(rawStr);
    }

//    public static void main(String[] args) {
//        String salt = RandomUtil.generateSalt(Constants.RANDOM_PLACE_NUM);
//        String encryptPwd = PasswordUtil.encrypt("seatunnel123", salt);
//        System.out.println(salt);
//        System.out.println(encryptPwd);
//    }
}
