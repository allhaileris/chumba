#import "Env.h"

@implementation Env

- (instancetype)initWithFile:(NSString *)path
{
    self = [super init];

    if (self)
        {
            NSError *error;
            _environment           = [[NSMutableDictionary alloc] init];
            NSString *fileContents = [NSString stringWithContentsOfFile:path
                                                               encoding:NSUTF8StringEncoding
                                                                  error:&error];

            NSArray *components = [fileContents componentsSeparatedByString:@"\n"];


            for (NSString *string in components)
                {
                    NSArray *parameter = [string componentsSeparatedByString:@"="];
                    NSString *key      = [parameter objectAtIndex:0];
                    NSString *value    = [parameter objectAtIndex:1];

                    [_environment setObject:value forKey:key];
                }
        }

    return self;
}

- (NSString *)getParameter:(NSString *)key
{
    return [_environment objectForKey:key];
}

@end
