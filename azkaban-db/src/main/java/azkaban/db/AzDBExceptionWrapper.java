/*
 * Copyright 2012 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.db;

import com.google.common.reflect.TypeToken;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
/**
 * Represents often fatal errors that occur within the database layer.
 */
public final class AzDBExceptionWrapper<E extends Throwable> {

//  private final Class<E> exceptionClass;
  private final String exceptionMessage;

  public AzDBExceptionWrapper(String message) {
    this.exceptionMessage = message;
//    this.exceptionClass = (Class<E>) getType();
  }

//  private final TypeToken<E> typeToken = new TypeToken<E>(getClass()) { };
//  private final Type type = typeToken.getType(); // or getRawType() to return Class<? super T>
//
//  public Type getType() {
//    return type;
//  }

//  public Class<E> getExceptionClass(){
//    return this.exceptionClass;
//  }

//  public String getExceptionMessage(){
//    return this.exceptionMessage;
//  }

  public E create() {
    try {
      Type sooper = getClass().getGenericSuperclass();
      Type t = ((ParameterizedType)sooper).getActualTypeArguments()[ 0 ];
      return (E)(Class.forName( t.getTypeName() ).getConstructor(String.class).newInstance(exceptionMessage));
    }
    catch( Exception e ) {
      return null;
    }
  }
}
