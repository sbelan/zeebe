package io.zeebe.exporter.spi;

import java.lang.annotation.*;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface ConfigArgument {
  String value() default "";
}
