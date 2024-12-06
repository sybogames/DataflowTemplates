/*
 * Copyright (C) 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.transforms;

import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.util.Objects;
import org.apache.beam.sdk.values.TupleTag;

final class AutoValueJSONTransformer<T> extends JSONTransformer<T> {

  private final TupleTag<FailsafeElement<T, String>> successTag;

  private final TupleTag<FailsafeElement<T, String>> failureTag;

  private AutoValueJSONTransformer(
      TupleTag<FailsafeElement<T, String>> successTag,
      TupleTag<FailsafeElement<T, String>> failureTag) {
    this.successTag = successTag;
    this.failureTag = failureTag;
  }

  @Override
  public TupleTag<FailsafeElement<T, String>> successTag() {
    return successTag;
  }

  @Override
  public TupleTag<FailsafeElement<T, String>> failureTag() {
    return failureTag;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof JSONTransformer) {
      JSONTransformer<?> that = (JSONTransformer<?>) o;
      return this.successTag.equals(that.successTag()) && this.failureTag.equals(that.failureTag());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(successTag, failureTag);
  }

  static final class Builder<T> extends JSONTransformer.Builder<T> {
    private TupleTag<FailsafeElement<T, String>> successTag;
    private TupleTag<FailsafeElement<T, String>> failureTag;

    Builder() {}

    @Override
    public JSONTransformer.Builder<T> setSuccessTag(
        TupleTag<FailsafeElement<T, String>> successTag) {
      if (successTag == null) {
        throw new NullPointerException("Null successTag");
      }
      this.successTag = successTag;
      return this;
    }

    @Override
    public JSONTransformer.Builder<T> setFailureTag(
        TupleTag<FailsafeElement<T, String>> failureTag) {
      if (failureTag == null) {
        throw new NullPointerException("Null failureTag");
      }
      this.failureTag = failureTag;
      return this;
    }

    @Override
    public JSONTransformer<T> build() {
      if (this.successTag == null || this.failureTag == null) {
        StringBuilder missing = new StringBuilder();
        if (this.successTag == null) {
          missing.append(" successTag");
        }
        if (this.failureTag == null) {
          missing.append(" failureTag");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValueJSONTransformer<T>(this.successTag, this.failureTag);
    }
  }
}
