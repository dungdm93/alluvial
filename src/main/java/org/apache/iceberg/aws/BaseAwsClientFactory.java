package org.apache.iceberg.aws;

import com.google.common.collect.Maps;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.util.PropertyUtil;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;

import java.util.Map;
import java.util.regex.Pattern;

public abstract class BaseAwsClientFactory {
    // Use LinkedHashMap to preserve order
    protected Map<String, AwsClientMutation> mutations = Maps.newLinkedHashMap();

    protected <B extends AwsClientBuilder<B, ?>> void applyMutations(B builder) {
        mutations.forEach((name, mutation) -> mutation.mutate(builder));
    }

    protected void initialize(String prefix, Map<String, String> properties) {
        var regex = Pattern.compile(prefix + "\\.mutation\\.(?<name>\\w+)\\.class$");
        PropertyUtil.filterProperties(properties, regex.asMatchPredicate())
                .forEach((key, klass) -> {
                    var m = regex.matcher(key);
                    if (!m.matches()) return;
                    var name = m.group("name");
                    var props = PropertyUtil.propertiesWithPrefix(properties, prefix + ".mutation." + name + ".");
                    var mutation = initializeMutation(prefix, name, klass, props);
                    mutations.put(name, mutation);
                });
    }

    private AwsClientMutation initializeMutation(
            String prefix, String name, String klass,
            Map<String, String> properties
    ) {
        AwsClientMutation mutation;
        try {
            var ctor = DynConstructors.builder(AwsClientMutation.class)
                    .loader(AwsClientMutation.class.getClassLoader())
                    .hiddenImpl(klass)
                    .<AwsClientMutation>buildChecked();
            mutation = ctor.newInstance();
        } catch (NoSuchMethodException e) {
            var msg = String.format("Cannot initialize %s mutation named %s with class %s", prefix, name, klass);
            throw new IllegalArgumentException(msg, e);
        }

        mutation.initialize(properties);
        return mutation;
    }
}
